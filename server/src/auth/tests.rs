// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use base64::Engine as _;
use jsonwebtoken::Algorithm;
use jsonwebtoken::jwk::{Jwk, JwkSet};
use tonic::{Code, Request, service::Interceptor as _};

use super::bearer::{MAX_TOKEN_FILE_BYTES, ScrubbedToken};
use super::oidc::{
    MAX_CANDIDATE_KEYS, OidcDiscoveryDocument, parse_worker_for_actor, validate_discovery_document,
};
use super::*;

struct TempTokenFile(std::path::PathBuf);
impl TempTokenFile {
    fn new(contents: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "jj-cc-auth-test-{}-{}.txt",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, contents).unwrap();
        Self(path)
    }
}
impl Drop for TempTokenFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn disabled_interceptor_allows_unauthenticated_requests() {
    let mut auth = BearerAuthInterceptor::from_tokens(Vec::<String>::new());
    assert!(!auth.is_enabled());
    assert!(auth.call(Request::new(())).is_ok());
}

#[test]
fn enabled_interceptor_enforces_tokens_and_supports_rotation() {
    let mut auth = BearerAuthInterceptor::from_tokens([" token-v1 ", "token-v2"]);
    assert!(auth.is_enabled());

    let err = auth.call(Request::new(())).unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    let mut wrong = Request::new(());
    wrong
        .metadata_mut()
        .insert("authorization", "Bearer wrong-secret".parse().unwrap());
    assert_eq!(auth.call(wrong).unwrap_err().code(), Code::Unauthenticated);

    for valid_token in ["token-v1", "token-v2"] {
        let mut ok_req = Request::new(());
        ok_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {valid_token}").parse().unwrap(),
        );
        assert!(auth.call(ok_req).is_ok());
    }
}

#[test]
fn bearer_scheme_is_case_insensitive_and_rejects_internal_spaces_and_hashes() {
    let mut auth = BearerAuthInterceptor::from_tokens(["secret-token"]);
    for header in [
        "Bearer secret-token",
        "bearer secret-token",
        "BEARER  secret-token",
    ] {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("authorization", header.parse().unwrap());
        assert!(auth.call(req).is_ok(), "{header}");
    }
    for header in [
        "Basic secret-token",
        "Bearersecret-token",
        "Bearer ",
        "Bearer secret token",
        "Bearer secret#token",
    ] {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("authorization", header.parse().unwrap());
        assert_eq!(
            auth.call(req).unwrap_err().code(),
            Code::Unauthenticated,
            "{header}"
        );
    }
}

#[test]
fn try_from_tokens_rejects_whitespace_hashes_and_oversized_tokens_without_leaking() {
    assert!(
        !BearerAuthInterceptor::try_from_tokens(["", " ", "\u{feff} , "])
            .unwrap()
            .is_enabled()
    );
    assert!(
        BearerAuthInterceptor::try_from_tokens(["\u{feff}clean-token"])
            .unwrap()
            .is_enabled()
    );

    let oversized = "x".repeat(MAX_BEARER_TOKEN_BYTES + 1);
    for bad in [
        "Bearer leak-me-123",
        "leak-me-123 extra",
        "leak-me-123\t1",
        "K#leak-me-123",
        oversized.as_str(),
    ] {
        let err = BearerAuthInterceptor::try_from_tokens([bad]).unwrap_err();
        assert!(!err.contains("leak-me-123"), "error leaked secret: {err}");
    }

    let auth = BearerAuthInterceptor::from_tokens(["super-secret-value"]);
    let debug = format!("{auth:?}");
    assert!(!debug.contains("super-secret-value"), "{debug}");
    assert!(debug.contains("token_count: 1"), "{debug}");
}

#[test]
fn duplicate_authorization_headers_are_rejected() {
    let mut auth = BearerAuthInterceptor::from_tokens(["secret-token"]);
    let mut req = Request::new(());
    req.metadata_mut()
        .append("authorization", "Bearer secret-token".parse().unwrap());
    req.metadata_mut()
        .append("authorization", "Bearer wrong-token".parse().unwrap());
    assert_eq!(auth.call(req).unwrap_err().code(), Code::Unauthenticated);
}

#[test]
fn bind_auth_policy_validates_brackets_and_requires_auth_on_non_loopback() {
    for loopback in ["127.0.0.1", "127.0.1.1", "::1", "[::1]", "localhost"] {
        assert!(is_loopback_host(loopback), "{loopback}");
        assert!(enforce_bind_auth_policy(loopback, false, false).is_ok());
    }
    for invalid_bracket in ["[127.0.0.1]", "[localhost]", "[::1", "::1]", ""] {
        assert!(!is_loopback_host(invalid_bracket), "{invalid_bracket}");
        assert!(enforce_bind_auth_policy(invalid_bracket, true, true).is_err());
    }
    for remote in ["0.0.0.0", "::", "[::]", "10.0.0.5", "my-server.internal"] {
        assert!(!is_loopback_host(remote), "{remote}");
        let err = enforce_bind_auth_policy(remote, false, false).unwrap_err();
        for expected in [
            "--auth-token-file",
            "JJ_CC_SERVER_AUTH_TOKEN_FILE",
            "--auth-token",
            "JJ_CC_SERVER_AUTH_TOKEN",
            "--allow-unauthenticated",
            "JJ_CC_ALLOW_UNAUTHENTICATED=true",
        ] {
            assert!(err.contains(expected), "missing {expected:?} in: {err}");
        }
        assert!(enforce_bind_auth_policy(remote, true, false).is_ok());
        assert!(enforce_bind_auth_policy(remote, false, true).is_ok());
    }
}

#[test]
fn load_tokens_from_file_strips_spaced_comments_and_rejects_embedded_hash_secrets() {
    let valid = TempTokenFile::new(
        "\u{feff}# rotation file\n  tok-a , # inline comment after comma\n tok-b , tok-c # trailing\n",
    );
    let parsed = load_tokens_from_file(&valid.0).unwrap();
    assert_eq!(parsed, vec!["tok-a", "tok-b", "tok-c"]);

    // Embedded `#` inside a secret must fail closed rather than silently truncating to `"K"`.
    let embedded_hash = TempTokenFile::new("K#secret-remainder-xyz\n");
    let hash_err = load_tokens_from_file(&embedded_hash.0).unwrap_err();
    assert!(!hash_err.contains("secret-remainder-xyz"), "{hash_err}");

    let empty = TempTokenFile::new("  # comment only \n  , # inline only \n");
    assert!(load_tokens_from_file(&empty.0).is_err());

    let spaced = TempTokenFile::new("tok-first-ok\ntok-with space\n");
    let err = load_tokens_from_file(&spaced.0).unwrap_err();
    assert!(!err.contains("tok-first-ok") && !err.contains("tok-with"));

    let oversized_content = "a".repeat((MAX_TOKEN_FILE_BYTES as usize) + 1);
    let oversized = TempTokenFile::new(&oversized_content);
    let size_err = load_tokens_from_file(&oversized.0).unwrap_err();
    assert!(size_err.contains("exceeds maximum size"), "{size_err}");

    let mut tokens = vec!["secret-1".to_string(), "secret-2".to_string()];
    scrub_tokens(&mut tokens);
    assert!(tokens.is_empty());
}

#[test]
fn grpc_path_classification_allows_only_known_read_only_and_health_paths() {
    assert!(is_health_check_path("/grpc.health.v1.Health/Check"));
    assert!(is_health_check_path("/grpc.health.v1.Health/Watch"));
    assert!(!is_health_check_path("/grpc.health.v1.Health/"));
    assert!(!is_health_check_path("/grpc.health.v1.Health/Check/extra"));
    assert!(!is_health_check_path(
        "/commit_cloud.backend.BackendService/ReadCommit"
    ));

    for read_only in [
        "/commit_cloud.backend.BackendService/ReadCommit",
        "/commit_cloud.backend.BackendService/ReadTree",
        "/commit_cloud.backend.BackendService/ReadFile",
        "/commit_cloud.backend.BackendService/ReadSymlink",
        "/commit_cloud.backend.BackendService/ListCommits",
        "/commit_cloud.backend.BackendService/ResolveCommitPrefix",
        "/commit_cloud.backend.BackendService/ResolveChangePrefix",
        "/commit_cloud.op_store.OpStoreService/ReadOperation",
        "/commit_cloud.op_store.OpStoreService/ReadView",
        "/commit_cloud.op_store.OpStoreService/GetOpHeads",
        "/commit_cloud.op_store.OpStoreService/ResolveOperationPrefix",
        "/commit_cloud.workspace.WorkspaceService/GetWorkspace",
        "/commit_cloud.workspace.WorkspaceService/ListWorkspaces",
        "/commit_cloud.workspace.WorkspaceService/CheckWorkingCopyChanges",
    ] {
        assert!(
            is_read_only_grpc_path(read_only),
            "expected read-only: {read_only}"
        );
    }

    for mutating_or_unknown in [
        "/commit_cloud.backend.BackendService/RegisterProject",
        "/commit_cloud.backend.BackendService/RegisterRepository",
        "/commit_cloud.backend.BackendService/DeleteRepository",
        "/commit_cloud.backend.BackendService/WriteCommit",
        "/commit_cloud.backend.BackendService/WriteTree",
        "/commit_cloud.backend.BackendService/WriteFile",
        "/commit_cloud.backend.BackendService/WriteFileStream",
        "/commit_cloud.backend.BackendService/WriteSymlink",
        "/commit_cloud.op_store.OpStoreService/WriteOperation",
        "/commit_cloud.op_store.OpStoreService/WriteView",
        "/commit_cloud.op_store.OpStoreService/UpdateOpHeads",
        "/commit_cloud.op_store.OpStoreService/ReconcileOpHeads",
        "/commit_cloud.op_store.OpStoreService/PublishOperation",
        "/commit_cloud.workspace.WorkspaceService/UpdateWorkspace",
        "/commit_cloud.workspace.WorkspaceService/DeleteWorkspace",
        "/commit_cloud.backend.BackendService/Read",
        "/commit_cloud.backend.BackendService/ReadCommit/extra",
        "/unknown.Service/ReadCommit",
        "/grpc.health.v1.Health/Check",
        "commit_cloud.backend.BackendService/ReadCommit",
    ] {
        assert!(
            !is_read_only_grpc_path(mutating_or_unknown),
            "expected non-read-only: {mutating_or_unknown}"
        );
    }
}

#[test]
fn worker_for_actor_claim_parsing_covers_all_json_shapes() {
    assert!(!parse_worker_for_actor(None));
    assert!(!parse_worker_for_actor(Some(&serde_json::Value::Null)));
    assert!(!parse_worker_for_actor(Some(&serde_json::json!(false))));
    assert!(!parse_worker_for_actor(Some(&serde_json::json!(""))));
    assert!(!parse_worker_for_actor(Some(&serde_json::json!("   "))));

    assert!(parse_worker_for_actor(Some(&serde_json::json!(true))));
    assert!(parse_worker_for_actor(Some(&serde_json::json!("worker-1"))));
    assert!(parse_worker_for_actor(Some(&serde_json::json!({}))));
    assert!(parse_worker_for_actor(Some(&serde_json::json!({
        "workerId": "w-123"
    }))));
    assert!(parse_worker_for_actor(Some(&serde_json::json!([]))));
    assert!(parse_worker_for_actor(Some(&serde_json::json!(["w"]))));
    assert!(parse_worker_for_actor(Some(&serde_json::json!(0))));
    assert!(parse_worker_for_actor(Some(&serde_json::json!(1))));
}

#[test]
fn oidc_and_k8s_config_validation_and_url_normalization() {
    assert!(OidcValidator::new(OidcAuthConfig::new("http://idp.example.com", "cloud-jj")).is_err());
    assert!(
        OidcValidator::new(OidcAuthConfig::new(
            "https://user:pw@idp.example.com",
            "cloud-jj"
        ))
        .is_err()
    );
    assert!(OidcValidator::new(OidcAuthConfig::new("https://idp.example.com", "  ")).is_err());
    assert!(OidcValidator::new(OidcAuthConfig::new("http://127.0.0.1:8081", "cloud-jj")).is_ok());
    assert!(
        OidcValidator::new(OidcAuthConfig::new(
            "https://idp.ate-system.svc",
            "cloud-jj"
        ))
        .is_ok()
    );

    let k8s_base =
        K8sTokenReviewValidator::new(K8sTokenReviewConfig::new("https://kubernetes.default.svc/"))
            .unwrap();
    assert_eq!(
        k8s_base.endpoint_url(),
        "https://kubernetes.default.svc/apis/authentication.k8s.io/v1/tokenreviews"
    );
    let k8s_full = K8sTokenReviewValidator::new(K8sTokenReviewConfig::new(
        "http://127.0.0.1:9443/apis/authentication.k8s.io/v1/tokenreviews",
    ))
    .unwrap();
    assert_eq!(
        k8s_full.endpoint_url(),
        "http://127.0.0.1:9443/apis/authentication.k8s.io/v1/tokenreviews"
    );
    assert!(
        K8sTokenReviewValidator::new(K8sTokenReviewConfig::new("http://10.0.0.1:6443")).is_err()
    );

    let mut empty_aud = K8sTokenReviewConfig::new("https://kubernetes.default.svc");
    empty_aud.audience = Some("  ".to_string());
    assert!(K8sTokenReviewValidator::new(empty_aud).is_err());

    let bad_ca = TempTokenFile::new("not a valid PEM certificate");
    let mut bad_ca_cfg = OidcAuthConfig::new("https://idp.example.com", "cloud-jj");
    bad_ca_cfg.ca_cert = Some(bad_ca.0.clone());
    assert!(OidcValidator::new(bad_ca_cfg).is_err());

    let empty_sa = TempTokenFile::new("   \n");
    let mut bad_sa_cfg = K8sTokenReviewConfig::new("https://kubernetes.default.svc");
    bad_sa_cfg.sa_token_file = Some(empty_sa.0.clone());
    assert!(K8sTokenReviewValidator::new(bad_sa_cfg).is_err());

    let mut cfg = ServerAuthConfig::default();
    assert!(!cfg.is_enabled());
    cfg.oidc = Some(OidcAuthConfig::new("https://idp.example.com", "cloud-jj"));
    assert!(cfg.is_enabled());
    cfg.oidc = None;
    cfg.k8s_tokenreview = Some(K8sTokenReviewConfig::new("https://kubernetes.default.svc"));
    assert!(cfg.is_enabled());
}

#[test]
fn enforce_actor_repo_suffix_handles_boundaries_and_caller_kinds() {
    let actor = CallerIdentity {
        subject: "actor-sub-1".to_string(),
        actor_uid: Some("uid-1".to_string()),
        actor_name: Some("coder-1".to_string()),
        atespace: Some("demo".to_string()),
        is_worker_for_actor: false,
        kind: CallerKind::OidcActor,
    };
    assert!(enforce_actor_repo_suffix(Some(&actor), true, "a1b2c3d4_demo_coder-1").is_ok());
    assert!(enforce_actor_repo_suffix(Some(&actor), false, "other-repo").is_ok());

    for disallowed in [
        "other-repo",
        "coder-1",
        "demo_coder-1",
        "demo-coder-1",
        "a1b2c3d4-demo-coder-1",
        "a1b2c3d4.demo.coder-1",
        "a1b2c3d4.demo_coder-1",
        "a1b2c3d4-demo_coder-1",
        "a1b2c3d4_demo-coder-1",
        "a1b2c3d4_demo.coder-1",
        "a1b2c3d4.demo_demo_coder-1",
        "a1b2-c3d4_demo_coder-1",
        "a1b2c3d4_extra_demo_coder-1",
        "a1b2c3d4_coder-1",
        "a1b2c3d4_demo_coder-2",
        "a1b2c3d4_other_coder-1",
        "evildemo_coder-1",
        "evilcoder-1",
        "_demo_coder-1",
        "a1b2c3d4__demo_coder-1",
        "a1b2c3d4_demo__coder-1",
        "",
    ] {
        assert_eq!(
            enforce_actor_repo_suffix(Some(&actor), true, disallowed)
                .unwrap_err()
                .code(),
            Code::PermissionDenied,
            "expected PERMISSION_DENIED for {disallowed:?}"
        );
    }

    // Hyphenated atespace + hyphenated actorName succeeds only with `<alnum>_<atespace>_<actorName>`.
    let prod_actor = CallerIdentity {
        subject: "actor-prod".to_string(),
        actor_uid: Some("uid-prod".to_string()),
        actor_name: Some("coder-1".to_string()),
        atespace: Some("prod-space".to_string()),
        is_worker_for_actor: false,
        kind: CallerKind::OidcActor,
    };
    assert!(
        enforce_actor_repo_suffix(Some(&prod_actor), true, "1234abcd_prod-space_coder-1").is_ok()
    );
    for disallowed in [
        "prod-space_coder-1",
        "prod-space-coder-1",
        "1234abcd-prod-space-coder-1",
        "1234abcd_other-space_coder-1",
    ] {
        assert_eq!(
            enforce_actor_repo_suffix(Some(&prod_actor), true, disallowed)
                .unwrap_err()
                .code(),
            Code::PermissionDenied,
            "expected PERMISSION_DENIED for prod_actor on {disallowed:?}"
        );
    }

    // Victim (atespace="demo", actorName="coder-1") vs attacker
    // (atespace="coder", actorName="1").
    let attacker_coder_1 = CallerIdentity {
        atespace: Some("coder".to_string()),
        actor_name: Some("1".to_string()),
        ..actor.clone()
    };
    for repo in [
        "a1b2c3d4_demo_coder-1",
        "demo_coder-1",
        "demo-coder-1",
        "a1b2c3d4-demo-coder-1",
    ] {
        assert_eq!(
            enforce_actor_repo_suffix(Some(&attacker_coder_1), true, repo)
                .unwrap_err()
                .code(),
            Code::PermissionDenied,
            "collision 1 must be denied on {repo:?}"
        );
    }

    // Victim (atespace="prod-space", actorName="coder-1") vs attacker
    // (atespace="space", actorName="coder-1").
    let attacker_space_coder = CallerIdentity {
        atespace: Some("space".to_string()),
        actor_name: Some("coder-1".to_string()),
        ..actor.clone()
    };
    for repo in [
        "1234abcd_prod-space_coder-1",
        "prod-space_coder-1",
        "prod-space-coder-1",
        "1234abcd-prod-space-coder-1",
    ] {
        assert_eq!(
            enforce_actor_repo_suffix(Some(&attacker_space_coder), true, repo)
                .unwrap_err()
                .code(),
            Code::PermissionDenied,
            "collision 2 must be denied on {repo:?}"
        );
    }

    // Victim (atespace="demo", actorName="coder-1") vs attacker with missing or
    // blank `atespace` must always fail closed.
    for missing_space in [None, Some("   ".to_string())] {
        let unscoped_attacker = CallerIdentity {
            atespace: missing_space,
            ..actor.clone()
        };
        for repo in [
            "a1b2c3d4_demo_coder-1",
            "demo_coder-1",
            "a1b2c3d4_coder-1",
            "1234abcd-coder-1",
            "coder-1",
        ] {
            assert_eq!(
                enforce_actor_repo_suffix(Some(&unscoped_attacker), true, repo)
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied,
                "collision 3 (missing atespace) must be denied on {repo:?}"
            );
        }
    }

    // Victim (atespace="prod", actorName="space-coder") vs attacker
    // (atespace="prod-space", actorName="coder"), and vice versa.
    let prod_space_coder_actor = CallerIdentity {
        atespace: Some("prod".to_string()),
        actor_name: Some("space-coder".to_string()),
        ..actor.clone()
    };
    let prod_space_and_coder_actor = CallerIdentity {
        atespace: Some("prod-space".to_string()),
        actor_name: Some("coder".to_string()),
        ..actor.clone()
    };
    assert!(
        enforce_actor_repo_suffix(
            Some(&prod_space_coder_actor),
            true,
            "a1b2c3d4_prod_space-coder"
        )
        .is_ok()
    );
    assert!(
        enforce_actor_repo_suffix(
            Some(&prod_space_and_coder_actor),
            true,
            "a1b2c3d4_prod-space_coder"
        )
        .is_ok()
    );
    assert_eq!(
        enforce_actor_repo_suffix(
            Some(&prod_space_and_coder_actor),
            true,
            "a1b2c3d4_prod_space-coder"
        )
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        enforce_actor_repo_suffix(
            Some(&prod_space_coder_actor),
            true,
            "a1b2c3d4_prod-space_coder"
        )
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );

    // Mixed delimiters and claims containing '_' fail closed.
    for mixed in [
        "a1b2c3d4.demo_coder-1",
        "a1b2c3d4-demo_coder-1",
        "a1b2c3d4_demo.coder-1",
        "a1b2c3d4_demo-coder-1",
    ] {
        assert_eq!(
            enforce_actor_repo_suffix(Some(&actor), true, mixed)
                .unwrap_err()
                .code(),
            Code::PermissionDenied,
            "collision 5 (mixed delimiter) must be denied on {mixed:?}"
        );
    }
    let underscore_atespace = CallerIdentity {
        atespace: Some("prod_space".to_string()),
        actor_name: Some("coder".to_string()),
        ..actor.clone()
    };
    assert_eq!(
        enforce_actor_repo_suffix(
            Some(&underscore_atespace),
            true,
            "a1b2c3d4_prod_space_coder"
        )
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );
    let underscore_actor_name = CallerIdentity {
        atespace: Some("prod".to_string()),
        actor_name: Some("space_coder".to_string()),
        ..actor.clone()
    };
    assert_eq!(
        enforce_actor_repo_suffix(
            Some(&underscore_actor_name),
            true,
            "a1b2c3d4_prod_space_coder"
        )
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );

    // Same actorName in different atespaces (`space-a` vs `space-b`) cannot cross-write.
    let space_a_actor = CallerIdentity {
        actor_name: Some("coder-1".to_string()),
        atespace: Some("space-a".to_string()),
        ..actor.clone()
    };
    let space_b_actor = CallerIdentity {
        actor_name: Some("coder-1".to_string()),
        atespace: Some("space-b".to_string()),
        ..actor.clone()
    };
    assert!(
        enforce_actor_repo_suffix(Some(&space_a_actor), true, "1234abcd_space-a_coder-1").is_ok()
    );
    assert_eq!(
        enforce_actor_repo_suffix(Some(&space_a_actor), true, "1234abcd_space-b_coder-1")
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        enforce_actor_repo_suffix(Some(&space_b_actor), true, "1234abcd_space-a_coder-1")
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );

    let missing_name_actor = CallerIdentity {
        actor_name: None,
        ..actor.clone()
    };
    assert_eq!(
        enforce_actor_repo_suffix(Some(&missing_name_actor), true, "a1b2c3d4_demo_coder-1")
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );

    for kind in [
        CallerKind::StaticToken,
        CallerKind::OidcUser,
        CallerKind::K8sServiceAccount,
    ] {
        let caller = CallerIdentity {
            subject: "other".to_string(),
            actor_uid: None,
            actor_name: None,
            atespace: None,
            is_worker_for_actor: false,
            kind,
        };
        assert!(enforce_actor_repo_suffix(Some(&caller), true, "any-repo").is_ok());
    }
    assert!(enforce_actor_repo_suffix(None, true, "any-repo").is_ok());
}

#[test]
fn scrubbed_token_exposes_value_and_redacts_debug() {
    let token = ScrubbedToken::new("super-secret-jwt".to_string());
    assert_eq!(token.as_str(), "super-secret-jwt");
    let debug = format!("{token:?}");
    assert!(!debug.contains("super-secret-jwt"), "{debug}");
    assert!(debug.contains("redacted"), "{debug}");
    drop(token);
}

#[test]
fn unverified_issuer_matches_ignores_header_algorithm() {
    let validator =
        OidcValidator::new(OidcAuthConfig::new("https://idp.example.com", "cloud-jj")).unwrap();
    let b64 = |val: &serde_json::Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(val).unwrap())
    };
    let matching_payload = b64(&serde_json::json!({
        "iss": "https://idp.example.com",
        "sub": "actor-1"
    }));
    let other_payload = b64(&serde_json::json!({
        "iss": "https://other.example.com",
        "sub": "actor-1"
    }));

    let es512_tok = format!(
        "{}.{}.dummysig",
        b64(&serde_json::json!({"alg": "ES512", "typ": "JWT"})),
        matching_payload
    );
    let alg_none_sig_tok = format!(
        "{}.{}.dummysig",
        b64(&serde_json::json!({"alg": "none", "typ": "JWT"})),
        matching_payload
    );
    let alg_none_empty_sig_tok = format!(
        "{}.{}.",
        b64(&serde_json::json!({"alg": "none", "typ": "JWT"})),
        matching_payload
    );
    let other_iss_tok = format!(
        "{}.{}.dummysig",
        b64(&serde_json::json!({"alg": "ES512", "typ": "JWT"})),
        other_payload
    );

    let padded_matching_payload = base64::engine::general_purpose::URL_SAFE.encode(
        serde_json::to_vec(&serde_json::json!({
            "iss": "https://idp.example.com",
            "sub": "actor-1"
        }))
        .unwrap(),
    );
    assert!(
        padded_matching_payload.ends_with('='),
        "test fixture must include trailing '=' padding"
    );
    let padded_matching_tok = format!(
        "{}.{}.dummysig",
        b64(&serde_json::json!({"alg": "RS256", "typ": "JWT"})),
        padded_matching_payload
    );
    let padded_other_payload = base64::engine::general_purpose::URL_SAFE.encode(
        serde_json::to_vec(&serde_json::json!({
            "iss": "https://other.example.com",
            "sub": "actor-12"
        }))
        .unwrap(),
    );
    assert!(
        padded_other_payload.ends_with('='),
        "other-issuer test fixture must include trailing '=' padding"
    );
    let padded_other_tok = format!(
        "{}.{}.dummysig",
        b64(&serde_json::json!({"alg": "RS256", "typ": "JWT"})),
        padded_other_payload
    );

    assert!(validator.unverified_issuer_matches(&es512_tok));
    assert!(validator.unverified_issuer_matches(&alg_none_sig_tok));
    assert!(validator.unverified_issuer_matches(&alg_none_empty_sig_tok));
    assert!(validator.unverified_issuer_matches(&padded_matching_tok));
    assert!(!validator.unverified_issuer_matches(&other_iss_tok));
    assert!(!validator.unverified_issuer_matches(&padded_other_tok));
    assert!(!validator.unverified_issuer_matches("hdr.===.sig"));
    assert!(!validator.unverified_issuer_matches(&format!(".{matching_payload}.sig")));
    assert!(!validator.unverified_issuer_matches(&format!("hdr.{matching_payload}")));
    assert!(!validator.unverified_issuer_matches(&format!("hdr.{matching_payload}.sig.extra")));
}

#[test]
fn find_candidate_keys_skips_malformed_jwks_before_applying_cap() {
    let malformed_jwk: Jwk = serde_json::from_value(serde_json::json!({
        "kty": "RSA",
        "use": "sig",
        "alg": "RS256",
        "n": "!!!not-base64!!!",
        "e": "AQAB"
    }))
    .unwrap();
    let valid_jwk: Jwk = serde_json::from_value(serde_json::json!({
        "kty": "RSA",
        "use": "sig",
        "alg": "RS256",
        "n": "AQAB",
        "e": "AQAB"
    }))
    .unwrap();

    let mut keys = vec![malformed_jwk.clone(); MAX_CANDIDATE_KEYS + 2];
    keys.push(valid_jwk.clone());
    let candidates = OidcValidator::find_candidate_keys(&JwkSet { keys }, None, Algorithm::RS256);
    assert_eq!(candidates.len(), 1);

    let mut many_keys = vec![malformed_jwk; MAX_CANDIDATE_KEYS + 2];
    many_keys.extend(std::iter::repeat_n(valid_jwk, MAX_CANDIDATE_KEYS + 3));
    let capped =
        OidcValidator::find_candidate_keys(&JwkSet { keys: many_keys }, None, Algorithm::RS256);
    assert_eq!(capped.len(), MAX_CANDIDATE_KEYS);
}

#[tokio::test]
async fn verify_detailed_distinguishes_backend_failures_from_invalid_credentials() {
    // Bind and immediately drop a loopback TCP listener so connections fail fast with
    // ECONNREFUSED rather than waiting for DNS or connect timeouts.
    let closed_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let closed_addr = closed_listener.local_addr().unwrap();
    drop(closed_listener);
    let closed_base = format!("http://{closed_addr}");

    let oidc = OidcValidator::new(OidcAuthConfig::new(&closed_base, "cloud-jj")).unwrap();
    assert!(matches!(
        oidc.verify_detailed("not-a-jwt").await,
        Err(VerifyError::InvalidCredential(_))
    ));

    let b64 = |val: &serde_json::Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(val).unwrap())
    };
    let matching_iss_jwt = format!(
        "{}.{}.dummysig",
        b64(&serde_json::json!({"alg": "RS256", "typ": "JWT"})),
        b64(&serde_json::json!({"iss": closed_base, "sub": "actor-1"}))
    );
    assert!(matches!(
        oidc.verify_detailed(&matching_iss_jwt).await,
        Err(VerifyError::Backend(_))
    ));
    assert!(matches!(
        oidc.verify_detailed(&matching_iss_jwt).await,
        Err(VerifyError::Backend(_))
    ));

    let k8s = K8sTokenReviewValidator::new(K8sTokenReviewConfig::new(&closed_base)).unwrap();
    assert!(matches!(
        k8s.verify_detailed("some-sa-token").await,
        Err(VerifyError::Backend(_))
    ));
}

#[test]
fn apply_commit_actor_attribution_overwrites_and_strips_spoofed_values() {
    let mut commit = cc_common::backend::Commit::default();
    commit
        .external_ids
        .insert(ACTOR_SUB_EXTERNAL_ID_KEY.to_string(), b"spoof-sub".to_vec());
    commit
        .external_ids
        .insert(ACTOR_UID_EXTERNAL_ID_KEY.to_string(), b"spoof-uid".to_vec());
    commit
        .external_ids
        .insert("git-sha1".to_string(), vec![0xaa; 20]);

    let actor = CallerIdentity {
        subject: "real-sub".to_string(),
        actor_uid: Some("real-uid".to_string()),
        actor_name: Some("agent".to_string()),
        atespace: Some("ns".to_string()),
        is_worker_for_actor: false,
        kind: CallerKind::OidcActor,
    };
    apply_commit_actor_attribution(Some(&actor), &mut commit);
    assert_eq!(
        commit.external_ids.get(ACTOR_SUB_EXTERNAL_ID_KEY),
        Some(&b"real-sub".to_vec())
    );
    assert_eq!(
        commit.external_ids.get(ACTOR_UID_EXTERNAL_ID_KEY),
        Some(&b"real-uid".to_vec())
    );
    assert_eq!(commit.external_ids.get("git-sha1"), Some(&vec![0xaa; 20]));

    // Identity without actor_uid clears any previously supplied actor-uid.
    let user_without_uid = CallerIdentity {
        subject: "human@example.com".to_string(),
        actor_uid: None,
        actor_name: None,
        atespace: None,
        is_worker_for_actor: false,
        kind: CallerKind::OidcUser,
    };
    apply_commit_actor_attribution(Some(&user_without_uid), &mut commit);
    assert_eq!(
        commit.external_ids.get(ACTOR_SUB_EXTERNAL_ID_KEY),
        Some(&b"human@example.com".to_vec())
    );
    assert!(!commit.external_ids.contains_key(ACTOR_UID_EXTERNAL_ID_KEY));

    // Static token and unauthenticated callers strip spoofed actor headers.
    commit
        .external_ids
        .insert(ACTOR_SUB_EXTERNAL_ID_KEY.to_string(), b"spoof".to_vec());
    commit
        .external_ids
        .insert(ACTOR_UID_EXTERNAL_ID_KEY.to_string(), b"spoof".to_vec());
    let static_caller = CallerIdentity {
        subject: "static-bearer-token".to_string(),
        actor_uid: None,
        actor_name: None,
        atespace: None,
        is_worker_for_actor: false,
        kind: CallerKind::StaticToken,
    };
    apply_commit_actor_attribution(Some(&static_caller), &mut commit);
    assert!(!commit.external_ids.contains_key(ACTOR_SUB_EXTERNAL_ID_KEY));
    assert!(!commit.external_ids.contains_key(ACTOR_UID_EXTERNAL_ID_KEY));
    assert_eq!(commit.external_ids.get("git-sha1"), Some(&vec![0xaa; 20]));

    commit
        .external_ids
        .insert(ACTOR_SUB_EXTERNAL_ID_KEY.to_string(), b"spoof".to_vec());
    apply_commit_actor_attribution(None, &mut commit);
    assert!(!commit.external_ids.contains_key(ACTOR_SUB_EXTERNAL_ID_KEY));
}

#[test]
fn discovery_document_requires_exact_issuer_and_blocks_https_downgrade() {
    // Missing `issuer` field fails deserialization.
    assert!(
        serde_json::from_str::<OidcDiscoveryDocument>(
            r#"{"jwks_uri":"https://idp.example.com/jwks"}"#
        )
        .is_err()
    );

    let doc = OidcDiscoveryDocument {
        issuer: "https://idp.example.com".to_string(),
        jwks_uri: "https://idp.example.com/jwks.json".to_string(),
    };
    assert_eq!(
        validate_discovery_document("https://idp.example.com", &doc).unwrap(),
        "https://idp.example.com/jwks.json"
    );

    // Trailing-slash mismatch fails exact issuer equality.
    let slash_doc = OidcDiscoveryDocument {
        issuer: "https://idp.example.com/".to_string(),
        jwks_uri: "https://idp.example.com/jwks.json".to_string(),
    };
    assert!(validate_discovery_document("https://idp.example.com", &slash_doc).is_err());

    // HTTPS issuer (including uppercase scheme) must reject a discovered HTTP loopback
    // jwks_uri downgrade.
    let downgrade_doc = OidcDiscoveryDocument {
        issuer: "https://idp.example.com".to_string(),
        jwks_uri: "http://127.0.0.1:8080/jwks.json".to_string(),
    };
    assert!(validate_discovery_document("https://idp.example.com", &downgrade_doc).is_err());
    let upper_downgrade_doc = OidcDiscoveryDocument {
        issuer: "HTTPS://idp.example.com".to_string(),
        jwks_uri: "http://127.0.0.1:8080/jwks.json".to_string(),
    };
    assert!(validate_discovery_document("HTTPS://idp.example.com", &upper_downgrade_doc).is_err());

    // HTTP loopback test issuer may use HTTP loopback jwks_uri.
    let loopback_doc = OidcDiscoveryDocument {
        issuer: "http://127.0.0.1:8080".to_string(),
        jwks_uri: "http://127.0.0.1:8080/jwks.json".to_string(),
    };
    assert_eq!(
        validate_discovery_document("http://127.0.0.1:8080", &loopback_doc).unwrap(),
        "http://127.0.0.1:8080/jwks.json"
    );
}

// Self-signed test CA certificate (public cert only; private key discarded at generation).
const TEST_CA_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBnDCCAUGgAwIBAgIUC3UI/l6tsu/bw8Gp3zpuDDIOrI8wCgYIKoZIzj0EAwIw
IjEgMB4GA1UEAwwXamotY29tbWl0LWNsb3VkLXRlc3QtY2EwIBcNMjYxMTI4MDk1
NjEyWhgPMjEyNjExMDQwOTU2MTJaMCIxIDAeBgNVBAMMF2pqLWNvbW1pdC1jbG91
ZC10ZXN0LWNhMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE6G6VnaGYUAo5u0YR
MDxuX9id8bbXJe+ps36UffpYfLM+56pxSVmjbbyp0URdFKP+zj7HB1FXGAaviaTf
OqTgE6NTMFEwHQYDVR0OBBYEFDI32B7+EeDOSi5TY0LCwevTCwIUMB8GA1UdIwQY
MBaAFDI32B7+EeDOSi5TY0LCwevTCwIUMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZI
zj0EAwIDSQAwRgIhAL01KoK+K9QYmq8DaqcWYCYjnmTFVze8U3DWgCUBGR7LAiEA
mZJ0rH7HZonvBSqqe13cpyxy/+CUJEaEjoqDrOViYkc=
-----END CERTIFICATE-----
";

#[test]
fn endpoint_url_validation_rejects_ascii_control_characters() {
    // Leading/trailing ASCII spaces still trim.
    let spaced =
        OidcValidator::new(OidcAuthConfig::new(" https://idp.example.com ", "cloud-jj")).unwrap();
    assert_eq!(spaced.issuer(), "https://idp.example.com");

    // Embedded and leading/trailing tab, CR, and LF must fail closed across all URL surfaces.
    for bad_url in [
        "ht\ttps://idp.example.com",
        "h\nttps://idp.example.com",
        "h\rttps://idp.example.com",
        "https://idp.example.com/\tjwks.json",
        "https://idp.example.com/jw\r\nks.json",
        "\thttps://idp.example.com",
        "https://idp.example.com\n",
    ] {
        assert!(
            OidcValidator::new(OidcAuthConfig::new(bad_url, "cloud-jj")).is_err(),
            "OIDC issuer must reject control chars: {bad_url:?}"
        );

        let mut explicit_jwks = OidcAuthConfig::new("https://idp.example.com", "cloud-jj");
        explicit_jwks.jwks_uri = Some(bad_url.to_string());
        assert!(
            OidcValidator::new(explicit_jwks).is_err(),
            "explicit OIDC jwks_uri must reject control chars: {bad_url:?}"
        );

        let discovered_doc = OidcDiscoveryDocument {
            issuer: "https://idp.example.com".to_string(),
            jwks_uri: bad_url.to_string(),
        };
        assert!(
            validate_discovery_document("https://idp.example.com", &discovered_doc).is_err(),
            "discovered OIDC jwks_uri must reject control chars: {bad_url:?}"
        );

        assert!(
            K8sTokenReviewValidator::new(K8sTokenReviewConfig::new(bad_url)).is_err(),
            "K8s TokenReview URL must reject control chars: {bad_url:?}"
        );
    }

    // Even if `validate_discovery_document` is called directly with an unvalidated control-char
    // HTTPS issuer, it must fail closed rather than bypassing the HTTP downgrade guard.
    let tab_issuer = "ht\ttps://idp.example.com";
    let downgrade_doc = OidcDiscoveryDocument {
        issuer: tab_issuer.to_string(),
        jwks_uri: "http://127.0.0.1:8080/jwks.json".to_string(),
    };
    assert!(validate_discovery_document(tab_issuer, &downgrade_doc).is_err());

    // Full TokenReview API path with embedded control characters must also be rejected.
    for bad_full in [
        "https://kubernetes.default.svc/apis/authentication.k8s.io/v1/token\treviews",
        "https://kubernetes.default.svc/apis/authentication.k8s.io/v1/token\nreviews",
        "https://kubernetes.default.svc/apis/authentication.k8s.io/v1/token\rreviews",
    ] {
        assert!(
            K8sTokenReviewValidator::new(K8sTokenReviewConfig::new(bad_full)).is_err(),
            "full TokenReview URL must reject control chars: {bad_full:?}"
        );
    }
}

#[test]
fn custom_pem_ca_cert_bundle_accepts_valid_and_rejects_oversized() {
    let valid_ca = TempTokenFile::new(TEST_CA_CERT_PEM);
    let mut oidc_cfg = OidcAuthConfig::new("https://idp.example.com", "cloud-jj");
    oidc_cfg.ca_cert = Some(valid_ca.0.clone());
    assert!(OidcValidator::new(oidc_cfg).is_ok());

    let mut k8s_cfg = K8sTokenReviewConfig::new("https://kubernetes.default.svc");
    k8s_cfg.ca_cert = Some(valid_ca.0.clone());
    assert!(K8sTokenReviewValidator::new(k8s_cfg).is_ok());

    let oversized_ca = TempTokenFile::new(&"a".repeat((MAX_CA_CERT_FILE_BYTES as usize) + 1));
    let mut oidc_oversized = OidcAuthConfig::new("https://idp.example.com", "cloud-jj");
    oidc_oversized.ca_cert = Some(oversized_ca.0.clone());
    let oidc_err = OidcValidator::new(oidc_oversized).unwrap_err();
    assert!(oidc_err.contains("exceeds maximum size"), "{oidc_err}");

    let mut k8s_oversized = K8sTokenReviewConfig::new("https://kubernetes.default.svc");
    k8s_oversized.ca_cert = Some(oversized_ca.0.clone());
    let k8s_err = K8sTokenReviewValidator::new(k8s_oversized).unwrap_err();
    assert!(k8s_err.contains("exceeds maximum size"), "{k8s_err}");

    let bad_ca = TempTokenFile::new("not a PEM certificate");
    let mut k8s_bad_ca = K8sTokenReviewConfig::new("https://kubernetes.default.svc");
    k8s_bad_ca.ca_cert = Some(bad_ca.0.clone());
    assert!(K8sTokenReviewValidator::new(k8s_bad_ca).is_err());
}

#[tokio::test]
async fn k8s_tokenreview_bounds_in_flight_entries_and_permit_wait_timeout() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let requests = Arc::new(AtomicUsize::new(0));
    let req_counter = Arc::clone(&requests);

    let app = axum::Router::new().route(
        "/apis/authentication.k8s.io/v1/tokenreviews",
        axum::routing::post(move || {
            let counter = Arc::clone(&req_counter);
            let mut rx = gate_rx.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let _ = rx.wait_for(|&open| open).await;
                (
                    axum::http::StatusCode::CREATED,
                    axum::Json(serde_json::json!({
                        "apiVersion": "authentication.k8s.io/v1",
                        "kind": "TokenReview",
                        "status": {
                            "authenticated": true,
                            "user": {
                                "username": "system:serviceaccount:ns:sa",
                                "uid": "sa-uid-1"
                            }
                        }
                    })),
                )
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let validator = Arc::new(
        K8sTokenReviewValidator::with_limits_for_test(
            K8sTokenReviewConfig::new(format!("http://{addr}")),
            Duration::from_secs(30),
            1,
            2,
            Duration::from_millis(120),
        )
        .unwrap(),
    );

    // Occupy the single outbound semaphore permit with `tok-1`.
    let flight_1 = tokio::spawn({
        let v = Arc::clone(&validator);
        async move { v.verify_detailed("tok-1").await }
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while requests.load(Ordering::SeqCst) < 1 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Fill the remaining `in_flight` slot with `tok-2` (blocked waiting for semaphore permit)
    // and coalesce a second caller onto `tok-2`.
    let flight_2 = tokio::spawn({
        let v = Arc::clone(&validator);
        async move { v.verify_detailed("tok-2").await }
    });
    while validator.in_flight_len() < 2 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let flight_2_waiter = tokio::spawn({
        let v = Arc::clone(&validator);
        async move { v.verify_detailed("tok-2").await }
    });
    tokio::task::yield_now().await;

    // A third distinct token must immediately fast-fail with the in-flight limit error while
    // `in_flight` stays capped at 2.
    let overflow_start = tokio::time::Instant::now();
    let overflow_err = validator
        .verify_detailed("tok-3")
        .await
        .expect_err("distinct token beyond max_in_flight must fast-fail");
    assert!(
        overflow_start.elapsed() < Duration::from_millis(80),
        "in-flight cap rejection should be immediate"
    );
    match overflow_err {
        VerifyError::Backend(msg) => {
            assert!(msg.contains("in-flight limit reached"), "{msg}");
        }
        other => panic!("expected Backend error, got {other:?}"),
    }

    // Both the queued leader (`tok-2`) and its coalesced waiter must time out via
    // `guard.complete` without issuing another outbound request or retrying onto the queue.
    let (res_2, res_2_waiter) = tokio::join!(flight_2, flight_2_waiter);
    for (label, res) in [
        ("leader", res_2.unwrap()),
        ("waiter", res_2_waiter.unwrap()),
    ] {
        match res {
            Err(VerifyError::Backend(msg)) => {
                assert!(
                    msg.contains("concurrency queue timed out"),
                    "{label}: unexpected message {msg}"
                );
            }
            other => panic!("{label}: expected queue timeout Backend error, got {other:?}"),
        }
    }
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "timed-out leader and coalesced waiter must not hit TokenReview server"
    );

    // Releasing `tok-1` drains `in_flight` back to zero and allows `tok-3` to succeed.
    let _ = gate_tx.send(true);
    let id_1 = flight_1.await.unwrap().expect("tok-1 should succeed");
    assert_eq!(id_1.subject, "system:serviceaccount:ns:sa");
    assert_eq!(validator.in_flight_len(), 0);

    let id_3 = validator
        .verify_detailed("tok-3")
        .await
        .expect("tok-3 should succeed once capacity frees");
    assert_eq!(id_3.subject, "system:serviceaccount:ns:sa");
    assert_eq!(validator.in_flight_len(), 0);

    server.abort();
}
