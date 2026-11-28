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

CREATE TABLE IF NOT EXISTS projects(
  project_id TEXT
  PRIMARY KEY,
  name TEXT UNIQUE);

CREATE TABLE IF NOT EXISTS repositories(
  repo_id TEXT
  PRIMARY KEY,
  project_id TEXT NOT NULL
  REFERENCES projects(project_id),
  name TEXT,
  op_heads_json TEXT NOT NULL);

CREATE INDEX IF NOT EXISTS idx_repositories_project_id
ON
  repositories(project_id);

CREATE TABLE IF NOT EXISTS commits(
  project_id TEXT NOT NULL,
  commit_id BLOB NOT NULL,
  change_id BLOB NOT NULL,
  generation INTEGER NOT NULL,
  proto_bytes BLOB NOT NULL,
  seq INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(project_id, commit_id));

CREATE INDEX IF NOT EXISTS idx_commits_change_id
ON
  commits(project_id, change_id);

CREATE INDEX IF NOT EXISTS idx_commits_project_seq
ON
  commits(project_id, seq);

CREATE TABLE IF NOT EXISTS trees(
  project_id TEXT NOT NULL,
  tree_id BLOB NOT NULL,
  PRIMARY KEY(project_id, tree_id));

CREATE TABLE IF NOT EXISTS tree_entries(
  project_id TEXT NOT NULL,
  tree_id BLOB NOT NULL,
  name TEXT NOT NULL,
  entry_proto BLOB NOT NULL,
  PRIMARY KEY(project_id, tree_id, name));

CREATE TABLE IF NOT EXISTS symlinks(
  project_id TEXT NOT NULL,
  symlink_id BLOB NOT NULL,
  target TEXT NOT NULL,
  PRIMARY KEY(project_id, symlink_id));

CREATE TABLE IF NOT EXISTS blobs(
  blob_id BLOB
  PRIMARY KEY,
  content_size INTEGER NOT NULL,
  inline_content BLOB,
  primary_location TEXT,
  replicated_locations_json TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS project_blobs(
  project_id TEXT NOT NULL,
  blob_id BLOB NOT NULL,
  PRIMARY KEY(project_id, blob_id));

CREATE TABLE IF NOT EXISTS views(
  repo_id TEXT NOT NULL,
  view_id BLOB NOT NULL,
  proto_bytes BLOB NOT NULL,
  PRIMARY KEY(repo_id, view_id));

CREATE TABLE IF NOT EXISTS operations(
  repo_id TEXT NOT NULL,
  op_id BLOB NOT NULL,
  proto_bytes BLOB NOT NULL,
  published INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(repo_id, op_id));

CREATE TABLE IF NOT EXISTS workspaces(
  repo_id TEXT NOT NULL,
  user_email TEXT NOT NULL,
  workspace_name TEXT NOT NULL,
  commit_id BLOB NOT NULL,
  operation_id BLOB NOT NULL,
  tree_id BLOB NOT NULL,
  tree_ids_json TEXT NOT NULL
  DEFAULT
    '[]',
  conflict_labels_json TEXT NOT NULL
  DEFAULT
    '[]',
    sparse_patterns_json TEXT NOT NULL,
  PRIMARY KEY(repo_id, user_email, workspace_name));
