// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![deny(unused_must_use)]
#![cfg_attr(feature = "map_first_last", feature(map_first_last))]

#[macro_use]
extern crate pest_derive;

#[cfg(test)]
#[macro_use]
extern crate maplit;

pub mod backend;
pub mod commit;
pub mod commit_builder;
pub mod conflicts;
pub mod dag_walk;
pub mod diff;
pub mod file_util;
pub mod files;
pub mod git;
pub mod git_backend;
pub mod gitignore;
pub mod hg_backend;
pub mod index;
pub mod index_store;
pub mod local_backend;
pub mod lock;
pub mod matchers;
pub mod nightly_shims;
pub mod op_heads_store;
pub mod op_store;
pub mod operation;
pub mod protos;
pub mod refs;
pub mod repo;
pub mod repo_path;
pub mod revset;
pub mod revset_graph_iterator;
pub mod rewrite;
pub mod settings;
pub mod simple_op_store;
pub mod stacked_table;
pub mod store;
pub mod testutils;
pub mod transaction;
pub mod tree;
pub mod tree_builder;
pub mod view;
pub mod working_copy;
pub mod workspace;
