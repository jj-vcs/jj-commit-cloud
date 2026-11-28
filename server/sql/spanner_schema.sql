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

-- GoogleSQL DDL for jj-commit-cloud on Cloud Spanner and Spanner Omni.
-- Unbounded object and history tables (Commits, Trees, Symlinks, ProjectBlobs, Views,
-- Operations) stay independent top-level tables with scope-first composite keys so Spanner
-- splits large projects and repositories independently without oversized parent subtrees.
-- Interleaving is reserved for bounded parent-child pairs read together: TreeEntries in
-- Trees and Workspaces in Repositories.

CREATE TABLE IF NOT EXISTS Projects(
  ProjectId STRING(128) NOT NULL,
  Name STRING(256))
  PRIMARY KEY(ProjectId);

CREATE UNIQUE NULL_FILTERED INDEX IF NOT EXISTS ProjectsByName
ON
  Projects(Name);

CREATE TABLE IF NOT EXISTS Commits(
  ProjectId STRING(128) NOT NULL,
  CommitId BYTES(64) NOT NULL,
  ChangeId BYTES(64) NOT NULL,
  Generation INT64 NOT NULL,
  ProtoBytes BYTES(MAX) NOT NULL,
  InsertedAt TIMESTAMP OPTIONS (allow_commit_timestamp = TRUE))
  PRIMARY KEY(ProjectId, CommitId);

CREATE INDEX IF NOT EXISTS CommitsByChangeId
ON
  Commits(ProjectId, ChangeId);

CREATE INDEX IF NOT EXISTS CommitsByInsertedAt
ON
  Commits(ProjectId, InsertedAt);

CREATE TABLE IF NOT EXISTS Trees(
  ProjectId STRING(128) NOT NULL,
  TreeId BYTES(64) NOT NULL,
  Ready BOOL NOT NULL)
  PRIMARY KEY(ProjectId, TreeId);

CREATE TABLE IF NOT EXISTS TreeEntries(
  ProjectId STRING(128) NOT NULL,
  TreeId BYTES(64) NOT NULL,
  Name STRING(1024) NOT NULL,
  EntryProto BYTES(MAX) NOT NULL)
  PRIMARY KEY(ProjectId, TreeId, Name),
  INTERLEAVE IN PARENT Trees
ON DELETE CASCADE;

CREATE TABLE IF NOT EXISTS Symlinks(
  ProjectId STRING(128) NOT NULL,
  SymlinkId BYTES(64) NOT NULL,
  Target STRING(MAX) NOT NULL)
  PRIMARY KEY(ProjectId, SymlinkId);

CREATE TABLE IF NOT EXISTS ProjectBlobs(
  ProjectId STRING(128) NOT NULL,
  BlobId BYTES(64) NOT NULL)
  PRIMARY KEY(ProjectId, BlobId);

CREATE TABLE IF NOT EXISTS Repositories(
  RepoId STRING(128) NOT NULL,
  ProjectId STRING(128) NOT NULL,
  Name STRING(256),
  OpHeads ARRAY < BYTES(64) > NOT NULL)
  PRIMARY KEY(RepoId);

CREATE NULL_FILTERED INDEX IF NOT EXISTS RepositoriesByName
ON
  Repositories(Name);

CREATE INDEX IF NOT EXISTS RepositoriesByProject
ON
  Repositories(ProjectId);

CREATE TABLE IF NOT EXISTS Views(
  RepoId STRING(128) NOT NULL,
  ViewId BYTES(64) NOT NULL,
  ProtoBytes BYTES(MAX) NOT NULL)
  PRIMARY KEY(RepoId, ViewId);

CREATE TABLE IF NOT EXISTS Operations(
  RepoId STRING(128) NOT NULL,
  OperationId BYTES(64) NOT NULL,
  ProtoBytes BYTES(MAX) NOT NULL,
  Published BOOL)
  PRIMARY KEY(RepoId, OperationId);

CREATE TABLE IF NOT EXISTS Workspaces(
  RepoId STRING(128) NOT NULL,
  WorkspaceName STRING(256) NOT NULL,
  User STRING(256) NOT NULL,
  CommitId BYTES(64) NOT NULL,
  OperationId BYTES(64) NOT NULL,
  TreeId BYTES(64) NOT NULL,
  TreeIds ARRAY < BYTES(64) > NOT NULL,
  ConflictLabels ARRAY < STRING(MAX) > NOT NULL,
  SparsePatterns ARRAY < STRING(MAX) > NOT NULL)
  PRIMARY KEY(RepoId, WorkspaceName),
  INTERLEAVE IN PARENT Repositories
ON DELETE CASCADE;

CREATE TABLE IF NOT EXISTS Blobs(
  BlobId BYTES(64) NOT NULL,
  ContentSize INT64 NOT NULL,
  InlineContent BYTES(MAX),
  PrimaryLocation STRING(1024),
  ReplicatedLocations ARRAY < STRING(1024) > NOT NULL)
  PRIMARY KEY(BlobId);
