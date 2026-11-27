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

use reqwest::StatusCode;

use super::super::config::*;
use super::super::sigv4::*;
use super::super::*;

const AWS_DOC_ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";

const AWS_DOC_SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

#[test]
fn aws_sigv4_matches_official_s3_get_put_and_query_test_vectors() {
    let creds = S3Credentials {
        access_key: AWS_DOC_ACCESS_KEY.to_string(),
        secret_key: AWS_DOC_SECRET_KEY.to_string(),
        session_token: None,
    };

    // AWS S3 SigV4 Example 1: GET Object with Range header.
    let get_out = sign_v4(
        "GET",
        "/test.txt",
        &[],
        &[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
        ],
        b"",
        "20130524",
        "20130524T000000Z",
        "us-east-1",
        &creds,
    );
    assert_eq!(
        sha256_hex(get_out.canonical_request.as_bytes()),
        "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972"
    );
    assert_eq!(
        get_out.signed_headers,
        "host;range;x-amz-content-sha256;x-amz-date"
    );
    assert_eq!(
        get_out.signature,
        "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
    );
    assert_eq!(
        get_out.authorization,
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
    );

    // AWS S3 SigV4 Example 2: PUT Object with payload and special URI character `$`.
    let put_out = sign_v4(
        "PUT",
        "/test$file.text",
        &[],
        &[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("date", "Fri, 24 May 2013 00:00:00 GMT"),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ],
        b"Welcome to Amazon S3.",
        "20130524",
        "20130524T000000Z",
        "us-east-1",
        &creds,
    );
    assert_eq!(
        put_out.payload_hash,
        "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
    );
    assert_eq!(
        sha256_hex(put_out.canonical_request.as_bytes()),
        "9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d"
    );
    assert_eq!(
        put_out.signature,
        "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
    );

    // AWS S3 SigV4 Example 3: GET Bucket (List Objects) with query parameters.
    let list_out = sign_v4(
        "GET",
        "/",
        &[("prefix", "J"), ("max-keys", "2")],
        &[("host", "examplebucket.s3.amazonaws.com")],
        b"",
        "20130524",
        "20130524T000000Z",
        "us-east-1",
        &creds,
    );
    assert_eq!(
        list_out.signature,
        "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
    );
}

#[test]
fn utc_timestamp_formatting_matches_known_dates() {
    assert_eq!(
        format_unix_secs_amz(0),
        ("19700101".to_string(), "19700101T000000Z".to_string())
    );
    // 2013-05-24T00:00:00Z
    assert_eq!(
        format_unix_secs_amz(1_369_353_600),
        ("20130524".to_string(), "20130524T000000Z".to_string())
    );
    // Leap day: 2024-02-29T23:59:59Z
    assert_eq!(
        format_unix_secs_amz(1_709_251_199),
        ("20240229".to_string(), "20240229T235959Z".to_string())
    );
}

#[test]
fn normalize_s3_bucket_enforces_s3_naming_rules() {
    assert_eq!(normalize_s3_bucket("my-bucket").unwrap(), "my-bucket");
    assert_eq!(
        normalize_s3_bucket("s3://my-bucket.123/").unwrap(),
        "my-bucket.123"
    );
    assert_eq!(normalize_s3_bucket("abc").unwrap(), "abc");
    let max_len = "a".repeat(63);
    assert_eq!(normalize_s3_bucket(&max_len).unwrap(), max_len);

    let too_long = "a".repeat(64);
    for invalid in [
        "",
        "ab",
        &too_long,
        "UPPER",
        "has_underscore",
        ".abc",
        "abc.",
        "-abc",
        "abc-",
        "foo.-bar",
        "foo-.bar",
        "a.-.b",
        "a..b",
        "192.168.1.1",
        "s3://my-bucket/extra",
        "s3://my-bucket//",
        "my-bucket\n",
        "my bucket",
        "gs://my-bucket",
    ] {
        let err = normalize_s3_bucket(invalid).unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidArgument(_)),
            "expected InvalidArgument for {invalid:?}, got {err:?}"
        );
    }
}

#[test]
fn config_validation_url_styles_peer_specs_and_secret_redaction() {
    // Standard AWS defaults to virtual-hosted style and no auto-create.
    let aws_cfg = S3ConnectConfig {
        bucket: "s3://jj-blobs-prod".to_string(),
        region: "eu-west-1".to_string(),
        access_key: Some("AKIAEXAMPLE".to_string()),
        secret_key: Some("TOP-SECRET-VALUE-999".to_string()),
        session_token: Some("SESSION-SECRET-888".to_string()),
        ..Default::default()
    };
    let resolved = resolve_s3_connect_config(&aws_cfg).unwrap();
    assert_eq!(resolved.scheme, "https");
    assert_eq!(
        resolved.request_host,
        "jj-blobs-prod.s3.eu-west-1.amazonaws.com"
    );
    assert!(!resolved.path_style);
    assert!(!resolved.auto_create_bucket);
    assert_eq!(resolved.location_uri, "s3://jj-blobs-prod");

    // Secret redaction in Debug output.
    let dbg = format!("{aws_cfg:?}");
    assert!(!dbg.contains("TOP-SECRET-VALUE-999"));
    assert!(!dbg.contains("SESSION-SECRET-888"));
    assert!(dbg.contains("<redacted>"));

    // Custom HTTP endpoint (e.g. MinIO/SeaweedFS) defaults to path-style + auto-create.
    let minio_cfg = S3ConnectConfig {
        bucket: "jj-local".to_string(),
        endpoint: Some("http://minio:9000/".to_string()),
        ..Default::default()
    };
    let resolved = resolve_s3_connect_config(&minio_cfg).unwrap();
    assert_eq!(resolved.scheme, "http");
    assert_eq!(resolved.request_host, "minio:9000");
    assert!(resolved.path_style);
    assert!(resolved.auto_create_bucket);
    assert_eq!(resolved.base_path, "");

    // Default port stripping (`:80` / `:443`) and single-encoded `base_path` canonicalization.
    let ep_443 = parse_s3_endpoint("HTTPS://s3.example.com:443/prefix/").unwrap();
    assert_eq!(ep_443.scheme, "https");
    assert_eq!(ep_443.host_header, "s3.example.com");
    assert_eq!(ep_443.base_path, "/prefix");
    let ep_encoded = parse_s3_endpoint("https://s3.example.com/my%20prefix/my%2bsub/").unwrap();
    assert_eq!(ep_encoded.base_path, "/my%20prefix/my%2Bsub");
    let ep_plus = parse_s3_endpoint("https://s3.example.com/a+b").unwrap();
    assert_eq!(ep_plus.base_path, "/a%2Bb");
    let ep_80 = parse_s3_endpoint("http://minio:80").unwrap();
    assert_eq!(ep_80.host_header, "minio");

    // IPv6 literals canonicalize to RFC 5952 form to match reqwest's wire Host header.
    let ep_v6 = parse_s3_endpoint("http://[0:0:0:0:0:0:0:1]:9000").unwrap();
    assert_eq!(ep_v6.host_header, "[::1]:9000");
    assert!(ep_v6.authority.is_loopback());

    // Virtual-hosted style with custom DNS endpoint works; IP endpoint rejected.
    let vh_custom = S3ConnectConfig {
        bucket: "my-bucket".to_string(),
        endpoint: Some("https://s3.internal.example:8443".to_string()),
        path_style: Some(false),
        ..Default::default()
    };
    assert_eq!(
        resolve_s3_connect_config(&vh_custom).unwrap().request_host,
        "my-bucket.s3.internal.example:8443"
    );
    let vh_ip = S3ConnectConfig {
        bucket: "my-bucket".to_string(),
        endpoint: Some("http://127.0.0.1:9000".to_string()),
        path_style: Some(false),
        ..Default::default()
    };
    assert!(matches!(
        vh_ip.validate(),
        Err(StoreError::InvalidArgument(_))
    ));

    // Half-configured credentials or orphan session token rejected without echoing secret.
    let half_ak = S3ConnectConfig {
        bucket: "my-bucket".to_string(),
        access_key: Some("AKIA".to_string()),
        ..Default::default()
    };
    assert!(matches!(
        half_ak.validate(),
        Err(StoreError::InvalidArgument(_))
    ));
    let half_sk = S3ConnectConfig {
        bucket: "my-bucket".to_string(),
        secret_key: Some("SECRET-SHOULD-NOT-LEAK".to_string()),
        ..Default::default()
    };
    let err = half_sk.validate().unwrap_err();
    assert!(matches!(err, StoreError::InvalidArgument(_)));
    assert!(!err.to_string().contains("SECRET-SHOULD-NOT-LEAK"));

    let orphan_tok = S3ConnectConfig {
        bucket: "my-bucket".to_string(),
        session_token: Some("TOKEN-SHOULD-NOT-LEAK".to_string()),
        ..Default::default()
    };
    let err = orphan_tok.validate().unwrap_err();
    assert!(matches!(err, StoreError::InvalidArgument(_)));
    assert!(!err.to_string().contains("TOKEN-SHOULD-NOT-LEAK"));

    // Invalid access keys (empty, whitespace, '/', ',', control chars, or non-ASCII) fail
    // fast during validation instead of failing later when constructing Authorization headers.
    for bad_ak in [
        "",
        "ak with space",
        "ak/slash",
        "ak,comma",
        "ak\nctrl",
        "ak-é",
    ] {
        let cfg = S3ConnectConfig {
            bucket: "my-bucket".to_string(),
            access_key: Some(bad_ak.to_string()),
            secret_key: Some("valid-secret-key".to_string()),
            ..Default::default()
        };
        assert!(
            matches!(cfg.validate(), Err(StoreError::InvalidArgument(_))),
            "expected InvalidArgument for access_key {bad_ak:?}"
        );
    }

    // Invalid endpoints and regions rejected.
    for bad_ep in [
        "",
        "s3.amazonaws.com",
        "ftp://127.0.0.1:9000",
        "http://user:pass@127.0.0.1:9000",
        "https://s3.amazonaws.com?foo=bar",
        "https://s3.amazonaws.com#frag",
        "http://127.0.0.1:9000\r\nX-Injected: 1",
        "http://127.0.0.1:0",
        "http://127.0.0.1:9000/a//b",
        "http://127.0.0.1:9000/../escape",
        "http://127.0.0.1:9000/%2e/prefix",
        "http://127.0.0.1:9000/s3/%2e%2e/escape",
        "http://127.0.0.1:9000/s3/.%2E/escape",
        "http://127.0.0.1:9000/a%2fb",
        "http://127.0.0.1:9000/bad%2",
        "http://127.0.0.1:9000/bad%ZZ",
        "http://127.0.0.1:9000/bad%0a",
        "https://foo.-bar.example.com",
        "https://foo-.bar.example.com",
        "https://a.-.example.com",
    ] {
        assert!(
            matches!(
                validate_s3_endpoint(bad_ep),
                Err(StoreError::InvalidArgument(_))
            ),
            "expected InvalidArgument for endpoint {bad_ep:?}"
        );
    }
    for bad_region in ["", "US-EAST-1", "-us-east-1", "us-east-1-", "us/east"] {
        let cfg = S3ConnectConfig {
            bucket: "my-bucket".to_string(),
            region: bad_region.to_string(),
            ..Default::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(StoreError::InvalidArgument(_))
        ));
    }
    let ctrl_region_err = validate_s3_region("us-west-2\r\n").unwrap_err();
    assert!(matches!(ctrl_region_err, StoreError::InvalidArgument(_)));
    let ctrl_region_msg = ctrl_region_err.to_string();
    assert!(
        ctrl_region_msg.contains("control characters")
            && !ctrl_region_msg.contains('\r')
            && !ctrl_region_msg.contains('\n'),
        "validate_s3_region must sanitize control chars: {ctrl_region_msg:?}"
    );

    // Peer spec parsing inherits defaults and allows per-peer region and endpoint overrides.
    let peer_same = parse_s3_peer_spec("s3://peer-bucket", &aws_cfg).unwrap();
    assert_eq!(peer_same.bucket, "s3://peer-bucket");
    assert_eq!(peer_same.region, "eu-west-1");
    assert_eq!(peer_same.endpoint, None);
    assert_eq!(peer_same.access_key.as_deref(), Some("AKIAEXAMPLE"));

    let peer_region = parse_s3_peer_spec("s3://peer-ap:ap-northeast-1", &aws_cfg).unwrap();
    assert_eq!(peer_region.bucket, "s3://peer-ap");
    assert_eq!(peer_region.region, "ap-northeast-1");
    assert_eq!(peer_region.endpoint, None);
    let resolved_peer_region = resolve_s3_connect_config(&peer_region).unwrap();
    assert_eq!(
        resolved_peer_region.request_host,
        "peer-ap.s3.ap-northeast-1.amazonaws.com"
    );
    assert!(!resolved_peer_region.path_style);

    let peer_override = parse_s3_peer_spec("peer-b=http://127.0.0.1:9002", &aws_cfg).unwrap();
    assert_eq!(peer_override.bucket, "peer-b");
    assert_eq!(peer_override.region, "eu-west-1");
    assert_eq!(
        peer_override.endpoint.as_deref(),
        Some("http://127.0.0.1:9002")
    );

    let peer_region_and_ep =
        parse_s3_peer_spec("peer-c:us-west-2=http://127.0.0.1:9003", &aws_cfg).unwrap();
    assert_eq!(peer_region_and_ep.bucket, "peer-c");
    assert_eq!(peer_region_and_ep.region, "us-west-2");
    assert_eq!(
        peer_region_and_ep.endpoint.as_deref(),
        Some("http://127.0.0.1:9003")
    );

    for invalid_peer in [
        "",
        "peer=",
        "peer-eu:",
        ":eu-west-1",
        "peer-eu:INVALID",
        "peer-eu:eu-west-1:extra",
        "s3://peer-eu/:eu-west-1",
        "s3://peer-eu:eu-west-1/",
        "peer-eu:eu-west-1=",
    ] {
        assert!(
            matches!(
                parse_s3_peer_spec(invalid_peer, &aws_cfg),
                Err(StoreError::InvalidArgument(_))
            ),
            "expected InvalidArgument for peer spec {invalid_peer:?}"
        );
    }
}

#[test]
fn object_key_enforces_1_to_64_byte_blob_ids_and_url_paths() {
    assert!(matches!(
        S3BlobStore::object_key(&[]),
        Err(StoreError::InvalidArgument(_))
    ));
    assert_eq!(
        S3BlobStore::object_key(&[0x0a, 0xbc, 0xde]).unwrap(),
        "blobs/0a/bc/0abcde"
    );
    assert!(S3BlobStore::object_key(&[0xab; 64]).is_ok());
    assert!(matches!(
        S3BlobStore::object_key(&[0xab; 65]),
        Err(StoreError::InvalidArgument(_))
    ));

    let client = Client::new();
    let path_store = S3BlobStore {
        inner: Arc::new(S3Inner {
            client: client.clone(),
            scheme: "https",
            request_host: "proxy.example.com:8443".to_string(),
            base_path: "/s3".to_string(),
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            path_style: true,
            location_uri: "s3://my-bucket".to_string(),
            credentials: None,
        }),
    };
    assert_eq!(path_store.bucket_path(), "/s3/my-bucket");
    assert_eq!(
        path_store.url_for_encoded_path(&path_store.object_path("blobs/ab/cd/abcd")),
        "https://proxy.example.com:8443/s3/my-bucket/blobs/ab/cd/abcd"
    );

    let vh_store = S3BlobStore {
        inner: Arc::new(S3Inner {
            client,
            scheme: "https",
            request_host: "my-bucket.s3.us-east-1.amazonaws.com".to_string(),
            base_path: String::new(),
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            path_style: false,
            location_uri: "s3://my-bucket".to_string(),
            credentials: None,
        }),
    };
    assert_eq!(vh_store.bucket_path(), "/");
    assert_eq!(
        vh_store.url_for_encoded_path(&vh_store.object_path("blobs/ab/cd/abcd")),
        "https://my-bucket.s3.us-east-1.amazonaws.com/blobs/ab/cd/abcd"
    );
}

#[test]
fn map_http_status_err_covers_s3_status_and_auth_xml_codes() {
    assert!(matches!(
        map_http_status_err(StatusCode::BAD_REQUEST, "GET", b"bad"),
        StoreError::InvalidArgument(_)
    ));
    for auth_xml in [
        b"<Error><Code>InvalidAccessKeyId</Code></Error>".as_slice(),
        b"<Error><Code>SignatureDoesNotMatch</Code></Error>".as_slice(),
        b"<Error><Code>ExpiredToken</Code></Error>".as_slice(),
        b"<Error><Code>InvalidToken</Code></Error>".as_slice(),
        b"<Error><Code>TokenRefreshRequired</Code></Error>".as_slice(),
    ] {
        assert!(
            matches!(
                map_http_status_err(StatusCode::BAD_REQUEST, "GET", auth_xml),
                StoreError::Unauthenticated(_)
            ),
            "expected 400 {} -> Unauthenticated",
            String::from_utf8_lossy(auth_xml)
        );
        assert!(
            matches!(
                map_http_status_err(StatusCode::FORBIDDEN, "GET", auth_xml),
                StoreError::Unauthenticated(_)
            ),
            "expected 403 {} -> Unauthenticated",
            String::from_utf8_lossy(auth_xml)
        );
    }
    assert!(matches!(
        map_http_status_err(
            StatusCode::BAD_REQUEST,
            "PUT",
            b"<Error><Code>RequestTimeout</Code><Message>Your socket connection to the server was not read from or written to within the timeout period.</Message></Error>"
        ),
        StoreError::Unavailable(_)
    ));
    // Auth XML code past the 512-char snippet truncation threshold still classifies accurately.
    let padded_expired = format!(
        "<Error><Message>{}</Message><Code>ExpiredToken</Code></Error>",
        "x".repeat(600)
    );
    assert!(matches!(
        map_http_status_err(StatusCode::BAD_REQUEST, "GET", padded_expired.as_bytes()),
        StoreError::Unauthenticated(_)
    ));
    assert!(matches!(
        map_http_status_err(StatusCode::UNAUTHORIZED, "GET", b""),
        StoreError::Unauthenticated(_)
    ));
    assert!(matches!(
        map_http_status_err(
            StatusCode::FORBIDDEN,
            "GET",
            b"<Error><Code>AccessDenied</Code></Error>"
        ),
        StoreError::PermissionDenied(_)
    ));
    for transient in [
        StatusCode::REQUEST_TIMEOUT,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::GATEWAY_TIMEOUT,
    ] {
        assert!(matches!(
            map_http_status_err(transient, "GET", b""),
            StoreError::Unavailable(_)
        ));
    }
    assert!(matches!(
        map_http_status_err(StatusCode::RANGE_NOT_SATISFIABLE, "GET", b""),
        StoreError::OutOfRange(_)
    ));
    assert!(matches!(
        map_http_status_err(StatusCode::TOO_MANY_REQUESTS, "GET", b""),
        StoreError::ResourceExhausted(_)
    ));
    assert!(matches!(
        map_http_status_err(StatusCode::TEMPORARY_REDIRECT, "GET", b""),
        StoreError::Backend(_)
    ));
    assert!(matches!(
        map_http_status_err(StatusCode::IM_A_TEAPOT, "GET", b""),
        StoreError::Backend(_)
    ));
}

#[test]
fn s3_endpoint_validation_never_echoes_embedded_userinfo() {
    for bad in [
        "http://ak:TOPSECRET@minio:9000",
        "http://ak:TOPSECRET @minio:9000",
        "http://ak:TOPSECRET@minio:9000?foo=bar",
        "http://ak:TOPSECRET@minio:9000\n",
    ] {
        let err = validate_s3_endpoint(bad).expect_err("endpoint with userinfo must fail");
        let msg = err.to_string();
        assert!(
            !msg.contains("TOPSECRET"),
            "S3 endpoint error leaked userinfo for {bad:?}: {msg}"
        );
    }
    for bad_bucket in [
        "http://ak:TOPSECRET@minio:9000",
        "http://ak:TOPSECRET@minio:9000\n",
        "http://ak:TOPSECRET @minio:9000",
        "s3://ak:TOPSECRET@peer",
    ] {
        let err =
            normalize_s3_bucket(bad_bucket).expect_err("bucket identifier with userinfo must fail");
        let msg = err.to_string();
        assert!(
            !msg.contains("TOPSECRET"),
            "normalize_s3_bucket error leaked userinfo for {bad_bucket:?}: {msg}"
        );
    }
    let defaults = S3ConnectConfig::default();
    for bad_peer in [
        "=http://ak:TOPSECRET@minio:9000",
        "http://ak:TOPSECRET@minio:9000",
        "http://ak:TOPSECRET=1@minio:9000",
        "alice:my-secret-pw=1@minio:9000",
        "https://ak:TOPSECRET?foo=bar@minio:9000",
        "s3://ak:TOPSECRET@peer=",
        "s3://ak:TOPSECRET@peer=http://minio:9000",
        "peer-bucket=http://ak:TOPSECRET@minio:9000",
        "peer-bucket:eu-west-1=http://ak:TOPSECRET@minio:9000",
        "peer-bucket:TOPSECRET=http://ak:PASS@minio:9000",
        "peer-bucket=https://s3.example.com?X-Amz-Signature=TOPSECRET",
        "peer-bucket:eu-west-1=https://s3.example.com?X-Amz-Signature=TOPSECRET",
        "peer-bucket=https://s3.example.com#TOPSECRET",
    ] {
        let err =
            parse_s3_peer_spec(bad_peer, &defaults).expect_err("peer spec with userinfo must fail");
        let msg = err.to_string();
        assert!(
            !msg.contains("TOPSECRET") && !msg.contains("my-secret-pw") && !msg.contains("PASS"),
            "parse_s3_peer_spec error leaked userinfo for {bad_peer:?}: {msg}"
        );
    }
    let dbg = format!(
        "{:?}",
        S3ConnectConfig {
            bucket: "s3://ak:TOPSECRET@peer".to_string(),
            endpoint: Some("http://ak:TOPSECRET@minio:9000".to_string()),
            ..Default::default()
        }
    );
    assert!(
        !dbg.contains("TOPSECRET"),
        "S3ConnectConfig Debug leaked userinfo: {dbg}"
    );

    // Query strings and fragments must also be masked so presigned tokens never leak in errors.
    for bad_qf in [
        "https://s3.example.com?X-Amz-Signature=TOPSECRET",
        "https://s3.example.com#TOPSECRET",
        "http://ak:PASS@minio:9000?token=TOPSECRET#FRAGSECRET",
        "http://ak:TOPSECRET?1@minio:9000",
        "https://s3.example.com?email=a@b&token=TOPSECRET",
    ] {
        let err = validate_s3_endpoint(bad_qf)
            .expect_err("endpoint with query/fragment/userinfo must fail");
        let msg = err.to_string();
        assert!(
            !msg.contains("TOPSECRET") && !msg.contains("FRAGSECRET") && !msg.contains("PASS"),
            "S3 endpoint error leaked query/fragment/userinfo for {bad_qf:?}: {msg}"
        );
    }

    // `redact_s3_peer_spec` masks userinfo, query strings, fragments, and ASCII control chars
    // across valid and malformed `<bucket>=<endpoint>` specs.
    for raw_spec in [
        "peer-a=https://s3.example.com?X-Amz-Signature=TOPSECRET",
        "peer-a:eu-west-1=https://s3.example.com?X-Amz-Signature=TOPSECRET",
        "peer-a=https://s3.example.com#TOPSECRET",
        "s3://peer-a=http://ak:PASS@minio:9000?token=TOPSECRET#FRAGSECRET",
        "s3://peer-a:eu-west-1=http://ak:PASS@minio:9000?token=TOPSECRET#FRAGSECRET",
        "peer-a=http://ak:TOPSECRET?a=b@minio:9000",
        "http://ak:TOPSECRET=1@minio:9000",
        "alice:my-secret-pw=1@minio:9000",
        "peer-a:TOPSECRET=http://ak:PASS@minio:9000",
        "https://ak:TOPSECRET?foo=bar@minio:9000",
        "ak:TOPSECRET@peer=https://minio:9000",
        "s3://ak:TOPSECRET@peer=https://ak2:FRAGSECRET@minio:9000",
        "peer-a=https://s3.example.com\r\n?X-Amz-Signature=TOPSECRET",
        "peer-a\r\n=http://ak:TOPSECRET@minio:9000",
    ] {
        let redacted = redact_s3_peer_spec(raw_spec);
        assert!(
            !redacted.contains("TOPSECRET")
                && !redacted.contains("FRAGSECRET")
                && !redacted.contains("PASS")
                && !redacted.contains("my-secret-pw"),
            "redact_s3_peer_spec leaked secret for {raw_spec:?}: {redacted:?}"
        );
        assert!(
            !redacted.bytes().any(|b| b.is_ascii_control()),
            "redact_s3_peer_spec left raw control char for {raw_spec:?}: {redacted:?}"
        );
    }
}

#[test]
fn endpoint_canonicalization_matches_url_and_validates_virtual_hosted_hosts() {
    // Non-canonical IPv4 shorthands canonicalize to dotted-quad IPv4 matching `url::Url` and
    // `reqwest`, are recognized as IP/loopback, and are rejected for virtual-hosted style.
    for shorthand in [
        "http://127.1:9000",
        "http://0177.0.0.1:9000",
        "http://2130706433:9000",
    ] {
        let ep = parse_s3_endpoint(shorthand).unwrap();
        assert_eq!(ep.host_header, "127.0.0.1:9000", "shorthand={shorthand}");
        assert!(ep.authority.is_ip, "shorthand={shorthand}");
        assert!(ep.authority.is_loopback(), "shorthand={shorthand}");

        let vh_cfg = S3ConnectConfig {
            bucket: "my-bucket".to_string(),
            endpoint: Some(shorthand.to_string()),
            path_style: Some(false),
            ..Default::default()
        };
        assert!(
            matches!(vh_cfg.validate(), Err(StoreError::InvalidArgument(_))),
            "virtual-hosted mode must reject IPv4 shorthand {shorthand}"
        );
    }

    // IPv4-mapped IPv6 canonicalizes to WHATWG `[::ffff:7f00:1]` matching `reqwest`'s wire
    // `Host` header, reports `is_ip` + `is_loopback`, and rejects virtual-hosted style.
    let v4_mapped = parse_s3_endpoint("http://[::ffff:127.0.0.1]:9000").unwrap();
    assert_eq!(v4_mapped.host_header, "[::ffff:7f00:1]:9000");
    assert!(v4_mapped.authority.is_ip);
    assert!(v4_mapped.authority.is_loopback());
    let vh_v4_mapped = S3ConnectConfig {
        bucket: "my-bucket".to_string(),
        endpoint: Some("http://[::ffff:127.0.0.1]:9000".to_string()),
        path_style: Some(false),
        ..Default::default()
    };
    assert!(matches!(
        vh_v4_mapped.validate(),
        Err(StoreError::InvalidArgument(_))
    ));

    // Numeric-final-label hostname (`s3.123`) is invalid under WHATWG URL parsing (`reqwest`)
    // and must fail validation upfront.
    assert!(matches!(
        validate_s3_endpoint("http://s3.123:9000"),
        Err(StoreError::InvalidArgument(_))
    ));
    assert!(matches!(
        validate_s3_endpoint("http://127.0.0.1:"),
        Err(StoreError::InvalidArgument(_))
    ));

    // Virtual-hosted `{bucket}.{host}` validates the resulting DNS hostname length/syntax.
    let long_host = format!(
        "{}.{}.{}.{}.example.com",
        "a".repeat(55),
        "b".repeat(55),
        "c".repeat(55),
        "d".repeat(55)
    );
    let too_long_vh = S3ConnectConfig {
        bucket: "a".repeat(40),
        endpoint: Some(format!("https://{long_host}:8443")),
        path_style: Some(false),
        ..Default::default()
    };
    assert!(matches!(
        too_long_vh.validate(),
        Err(StoreError::InvalidArgument(_))
    ));
    // The same long host works in path-style mode (where `{bucket}.` is not prepended).
    let ok_path_style = S3ConnectConfig {
        path_style: Some(true),
        ..too_long_vh
    };
    assert!(ok_path_style.validate().is_ok());
}
