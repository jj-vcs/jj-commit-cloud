-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Native PostgreSQL 17 DDL for jj-commit-cloud.
-- Shared immutable DAG objects (commits, trees, tree_entries, symlinks, project_blobs)
-- scope under projects(project_id), while per-repository mutable coordination
-- (op_heads, views, operations, workspaces) cascades cleanly on repository deletion.
-- Ordered text columns (tree_entries.name, workspaces.user_email, workspaces.workspace_name)
-- declare COLLATE "C" so B-tree indexes and keyset pagination match Rust UTF-8 byte order
-- regardless of database locale.

CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  name TEXT,
  commit_seq BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE projects
  ADD COLUMN IF NOT EXISTS commit_seq BIGINT NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_name
  ON projects (name)
  WHERE name IS NOT NULL;

CREATE TABLE IF NOT EXISTS repositories (
  repo_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  name TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_repositories_project_id
  ON repositories (project_id);

CREATE TABLE IF NOT EXISTS op_heads (
  repo_id TEXT NOT NULL REFERENCES repositories(repo_id) ON DELETE CASCADE,
  op_id BYTEA NOT NULL,
  added_seq BIGSERIAL NOT NULL,
  PRIMARY KEY (repo_id, op_id)
);

CREATE INDEX IF NOT EXISTS idx_op_heads_repo_seq
  ON op_heads (repo_id, added_seq, op_id);

CREATE TABLE IF NOT EXISTS commits (
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  commit_id BYTEA NOT NULL,
  change_id BYTEA NOT NULL,
  generation BIGINT NOT NULL,
  seq BIGINT NOT NULL,
  proto_bytes BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (project_id, commit_id)
);

CREATE INDEX IF NOT EXISTS commits_by_change_id
  ON commits (project_id, change_id, commit_id);

CREATE INDEX IF NOT EXISTS commits_by_seq
  ON commits (project_id, seq, commit_id);

UPDATE projects p
SET commit_seq = GREATEST(p.commit_seq, c.max_seq)
FROM (SELECT project_id, MAX(seq) AS max_seq FROM commits GROUP BY project_id) c
WHERE p.project_id = c.project_id
  AND p.commit_seq < c.max_seq;

CREATE TABLE IF NOT EXISTS trees (
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  tree_id BYTEA NOT NULL,
  ready BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (project_id, tree_id)
);

CREATE TABLE IF NOT EXISTS tree_entries (
  project_id TEXT NOT NULL,
  tree_id BYTEA NOT NULL,
  name TEXT COLLATE "C" NOT NULL,
  proto_bytes BYTEA NOT NULL,
  PRIMARY KEY (project_id, tree_id, name),
  FOREIGN KEY (project_id, tree_id)
    REFERENCES trees(project_id, tree_id)
    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS symlinks (
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  symlink_id BYTEA NOT NULL,
  target TEXT NOT NULL,
  PRIMARY KEY (project_id, symlink_id)
);

CREATE TABLE IF NOT EXISTS blobs (
  blob_id BYTEA PRIMARY KEY,
  content_size BIGINT NOT NULL,
  inline_content BYTEA,
  primary_location TEXT,
  replicated_locations TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS project_blobs (
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  blob_id BYTEA NOT NULL REFERENCES blobs(blob_id),
  PRIMARY KEY (project_id, blob_id)
);

CREATE TABLE IF NOT EXISTS views (
  repo_id TEXT NOT NULL REFERENCES repositories(repo_id) ON DELETE CASCADE,
  view_id BYTEA NOT NULL,
  proto_bytes BYTEA NOT NULL,
  PRIMARY KEY (repo_id, view_id)
);

CREATE TABLE IF NOT EXISTS operations (
  repo_id TEXT NOT NULL REFERENCES repositories(repo_id) ON DELETE CASCADE,
  op_id BYTEA NOT NULL,
  published BOOLEAN NOT NULL DEFAULT FALSE,
  proto_bytes BYTEA NOT NULL,
  PRIMARY KEY (repo_id, op_id)
);

CREATE TABLE IF NOT EXISTS workspaces (
  repo_id TEXT NOT NULL REFERENCES repositories(repo_id) ON DELETE CASCADE,
  user_email TEXT COLLATE "C" NOT NULL,
  workspace_name TEXT COLLATE "C" NOT NULL,
  operation_id BYTEA NOT NULL,
  commit_id BYTEA NOT NULL,
  tree_ids BYTEA[] NOT NULL,
  conflict_labels TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  sparse_patterns TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  sparse_patterns_set BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at BIGINT NOT NULL,
  PRIMARY KEY (repo_id, user_email, workspace_name)
);
