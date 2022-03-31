// Copyright 2022 Google LLC
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

use std::fmt::{Debug, Error, Formatter};
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use bstr::ByteSlice;
use hg::changelog::ChangelogRevisionData;
use hg::node::NODE_BYTES_LENGTH;
use hg::repo::RepoError;
use hg::utils::hg_path::HgPathBuf;
use hg::{Node, NodePrefix};
use protobuf::Message;

use crate::backend::{
    Backend, BackendError, BackendResult, ChangeId, Commit, CommitId, Conflict, ConflictId,
    ConflictPart, FileId, MillisSinceEpoch, Signature, SymlinkId, Timestamp, Tree, TreeId,
    TreeValue,
};
use crate::repo_path::{RepoPath, RepoPathComponent};
use crate::stacked_table::{TableSegment, TableStore};

const HASH_LENGTH: usize = NODE_BYTES_LENGTH;
const CONFLICT_SUFFIX: &str = ".jjconflict";

impl From<hg::repo::RepoError> for BackendError {
    fn from(err: hg::repo::RepoError) -> Self {
        match err {
            RepoError::NotFound { .. } => BackendError::NotFound,
            _ => BackendError::Other(format!("hg repo error: {:?}", err)),
        }
    }
}

impl From<hg::errors::HgError> for BackendError {
    fn from(err: hg::errors::HgError) -> Self {
        BackendError::Other(format!("hg error: {:?}", err))
    }
}

impl From<hg::revlog::revlog::RevlogError> for BackendError {
    fn from(_err: hg::revlog::revlog::RevlogError) -> Self {
        BackendError::Other("hg revlog error".to_string())
    }
}

pub struct HgBackend {
    repo: Mutex<hg::repo::Repo>,
    empty_tree_id: TreeId,
    overlay_store: TableStore,
    extra_metadata_store: TableStore,
}

impl HgBackend {
    fn new(
        repo: hg::repo::Repo,
        overlay_store: TableStore,
        extra_metadata_store: TableStore,
    ) -> Self {
        let empty_tree_id =
            TreeId::new(hex::decode("4b825dc642cb6eb9a060e54bf8d69288fbee4904").unwrap());
        HgBackend {
            repo: Mutex::new(repo),
            empty_tree_id,
            overlay_store,
            extra_metadata_store,
        }
    }

    pub fn init_external(store_path: PathBuf, hg_repo_path: PathBuf) -> Self {
        let mut hg_target_file = File::create(store_path.join("hg_target")).unwrap();
        hg_target_file
            .write_all(hg_repo_path.to_str().unwrap().as_bytes())
            .unwrap();
        let config = hg::config::Config::empty();
        let repo = hg::repo::Repo::find(&config, Some(store_path.join(hg_repo_path))).unwrap();
        let overlay_path = store_path.join("overlay");
        std::fs::create_dir(&overlay_path).unwrap();
        let overlay_store = TableStore::init(overlay_path, HASH_LENGTH);
        let extra_path = store_path.join("extra");
        std::fs::create_dir(&extra_path).unwrap();
        let extra_metadata_store = TableStore::init(extra_path, HASH_LENGTH);
        HgBackend::new(repo, overlay_store, extra_metadata_store)
    }

    pub fn load(store_path: PathBuf) -> Self {
        let mut hg_target_file = File::open(store_path.join("hg_target")).unwrap();
        let mut buf = Vec::new();
        hg_target_file.read_to_end(&mut buf).unwrap();
        let hg_repo_path_str = String::from_utf8(buf).unwrap();
        let hg_repo_path = store_path.join(hg_repo_path_str).canonicalize().unwrap();
        let config = hg::config::Config::empty();
        let repo = hg::repo::Repo::find(&config, Some(hg_repo_path)).unwrap();
        let extra_metadata_store = TableStore::load(store_path.join("extra"), HASH_LENGTH);
        let overlay_store = TableStore::load(store_path.join("overlay"), HASH_LENGTH);
        HgBackend::new(repo, overlay_store, extra_metadata_store)
    }
}

fn deserialize_commit(
    changeset: ChangelogRevisionData,
    change_id: ChangeId,
    parents: Vec<CommitId>,
) -> Commit {
    let tree_id = TreeId::from_bytes(changeset.manifest_node().unwrap().as_bytes());

    let author = changeset.user();
    let email_start = author.find(b"<");
    let name;
    let email;
    if email_start.is_some() && author.ends_with(b">") {
        name = String::from_utf8_lossy(&author[..email_start.unwrap()]).to_string();
        email =
            String::from_utf8_lossy(&author[email_start.unwrap() + 1..author.len() - 1]).to_string()
    } else {
        name = String::from_utf8_lossy(author).to_string();
        email = "".to_string();
    };
    let mut timestamp_iter = changeset.timestamp_line().splitn(3, |b| *b == b' ');

    let epoch_seconds: u64 = timestamp_iter
        .next()
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let offset_seconds: i32 = timestamp_iter
        .next()
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let timestamp = Timestamp {
        timestamp: MillisSinceEpoch(epoch_seconds * 1000),
        tz_offset: -offset_seconds / 60,
    };
    let signature = Signature {
        name,
        email,
        timestamp,
    };

    let description = String::from_utf8_lossy(changeset.description()).to_string();

    Commit {
        parents,
        predecessors: vec![],
        root_tree: tree_id,
        change_id,
        description,
        author: signature.clone(),
        committer: signature,
        is_open: false,
    }
}

fn serialize_extras(commit: &Commit) -> Vec<u8> {
    let mut proto = crate::protos::store::Commit::new();
    proto.is_open = commit.is_open;
    proto.change_id = commit.change_id.to_bytes();
    for predecessor in &commit.predecessors {
        proto.predecessors.push(predecessor.to_bytes());
    }
    proto.write_to_bytes().unwrap()
}

fn deserialize_extras(commit: &mut Commit, bytes: &[u8]) {
    let mut cursor = Cursor::new(bytes);
    let proto: crate::protos::store::Commit = Message::parse_from_reader(&mut cursor).unwrap();
    commit.is_open = proto.is_open;
    commit.change_id = ChangeId::new(proto.change_id);
    for predecessor in &proto.predecessors {
        commit.predecessors.push(CommitId::from_bytes(predecessor));
    }
}

impl Debug for HgBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_struct("HgBackend")
            .field("path", &self.repo.lock().unwrap().working_directory_path())
            .finish()
    }
}

fn to_hg_path(repo_path: &RepoPath) -> HgPathBuf {
    HgPathBuf::from_bytes(repo_path.to_internal_file_string().as_bytes())
}

fn to_hg_node(bytes: &[u8]) -> Node {
    let mut node_data = [0; NODE_BYTES_LENGTH];
    node_data.copy_from_slice(bytes);
    Node::from(&node_data)
}

impl Backend for HgBackend {
    fn hash_length(&self) -> usize {
        HASH_LENGTH
    }

    fn git_repo(&self) -> Option<git2::Repository> {
        None
    }

    fn hg_repo(&self) -> Option<hg::repo::Repo> {
        let path = self
            .repo
            .lock()
            .unwrap()
            .working_directory_path()
            .to_owned();
        let config = hg::config::Config::empty();
        Some(hg::repo::Repo::find(&config, Some(path)).unwrap())
    }

    fn read_file(&self, path: &RepoPath, id: &FileId) -> BackendResult<Box<dyn Read>> {
        if id.as_bytes().len() != self.hash_length() {
            return Err(BackendError::NotFound);
        }
        let locked_repo = self.repo.lock().unwrap();
        let filelog = locked_repo.filelog(&to_hg_path(path))?;
        let file_revision = filelog.data_for_node(to_hg_node(id.as_bytes()))?;
        let content = file_revision.file_data()?.to_owned();
        Ok(Box::new(Cursor::new(content)))
    }

    fn write_file(&self, _path: &RepoPath, _contents: &mut dyn Read) -> BackendResult<FileId> {
        unimplemented!("cannot write files to the hg backend");
    }

    fn read_symlink(&self, path: &RepoPath, id: &SymlinkId) -> Result<String, BackendError> {
        if id.as_bytes().len() != self.hash_length() {
            return Err(BackendError::NotFound);
        }
        let locked_repo = self.repo.lock().unwrap();
        let filelog = locked_repo.filelog(&to_hg_path(path))?;
        let file_revision = filelog.data_for_node(to_hg_node(id.as_bytes()))?;
        let content = file_revision.file_data()?.to_owned();
        let target = String::from_utf8(content).unwrap();
        Ok(target)
    }

    fn write_symlink(&self, _path: &RepoPath, _target: &str) -> Result<SymlinkId, BackendError> {
        unimplemented!("cannot write symlinks to the hg backend");
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn read_tree(&self, path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if id == &self.empty_tree_id {
            return Ok(Tree::default());
        }
        if id.as_bytes().len() != self.hash_length() {
            return Err(BackendError::NotFound);
        }

        if !path.is_root() {
            panic!("Tree manifests not supported yet");
        }

        let locked_repo = self.repo.lock().unwrap();
        let manifestlog = locked_repo.manifestlog()?;
        let manifest = manifestlog.data_for_node(NodePrefix::from(to_hg_node(id.as_bytes())))?;
        let mut tree = Tree::default();
        for entry in manifest.iter() {
            let entry = entry?;
            let name = entry.path.as_bytes().to_str().unwrap();
            let (name, value) = match &entry.flags {
                None => {
                    let id = FileId::from_hex(entry.hex_node_id.to_str().unwrap());
                    if name.ends_with(CONFLICT_SUFFIX) {
                        (
                            &name[0..name.len() - CONFLICT_SUFFIX.len()],
                            TreeValue::Conflict(ConflictId::from_hex(
                                entry.hex_node_id.to_str().unwrap(),
                            )),
                        )
                    } else {
                        (
                            name,
                            TreeValue::Normal {
                                id,
                                executable: false,
                            },
                        )
                    }
                }
                Some(b'x') => {
                    let id = FileId::from_hex(entry.hex_node_id.to_str().unwrap());
                    (
                        name,
                        TreeValue::Normal {
                            id,
                            executable: true,
                        },
                    )
                }
                Some(flag) => {
                    panic!("unexpected manifest flag {:?}", flag)
                }
            };
            tree.set(RepoPathComponent::from(name), value);
        }
        Ok(tree)
    }

    fn write_tree(&self, _path: &RepoPath, _contents: &Tree) -> BackendResult<TreeId> {
        unimplemented!("cannot write trees to the hg backend");
    }

    fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if id.as_bytes().len() != self.hash_length() {
            return Err(BackendError::NotFound);
        }

        let locked_repo = self.repo.lock().unwrap();
        let changelog = locked_repo.changelog()?;
        let rev = changelog.rev_from_node(NodePrefix::from(to_hg_node(id.as_bytes())))?;
        let revlog_entry = changelog.entry_for_rev(rev)?;
        let changeset = changelog.data_for_rev(rev)?;
        // We reverse the bits of the commit id to create the change id. We don't want
        // to use the first bytes unmodified because then it would be ambiguous
        // if a given hash prefix refers to the commit id or the change id. It
        // would have been enough to pick the last 16 bytes instead of the
        // leading 16 bytes to address that. We also reverse the bits to make it less
        // likely that users depend on any relationship between the two ids.
        let change_id = ChangeId::new(
            id.as_bytes()[4..HASH_LENGTH]
                .iter()
                .rev()
                .map(|b| b.reverse_bits())
                .collect(),
        );
        let mut parents = vec![];
        if let Some(p1_entry) = revlog_entry.p1_entry()? {
            parents.push(CommitId::from_bytes(p1_entry.node().as_bytes()))
        }
        if let Some(p2_entry) = revlog_entry.p2_entry()? {
            parents.push(CommitId::from_bytes(p2_entry.node().as_bytes()))
        }

        let mut commit = deserialize_commit(changeset, change_id, parents);

        let table = self.extra_metadata_store.get_head()?;
        let maybe_extras = table.get_value(id.as_bytes());
        if let Some(extras) = maybe_extras {
            deserialize_extras(&mut commit, extras);
        }

        Ok(commit)
    }

    fn write_commit(&self, _contents: &Commit) -> BackendResult<CommitId> {
        unimplemented!("cannot write commits to the hg backend");
    }

    fn read_conflict(&self, _path: &RepoPath, id: &ConflictId) -> BackendResult<Conflict> {
        let mut file = self.read_file(
            &RepoPath::from_internal_string("unused"),
            &FileId::new(id.to_bytes()),
        )?;
        let mut data = String::new();
        file.read_to_string(&mut data)?;
        let json: serde_json::Value = serde_json::from_str(&data).unwrap();
        Ok(Conflict {
            removes: conflict_part_list_from_json(json.get("removes").unwrap()),
            adds: conflict_part_list_from_json(json.get("adds").unwrap()),
        })
    }

    fn write_conflict(&self, _path: &RepoPath, _conflict: &Conflict) -> BackendResult<ConflictId> {
        unimplemented!("cannot write conflicts to the hg backend");
    }
}

fn conflict_part_list_to_json(parts: &[ConflictPart]) -> serde_json::Value {
    serde_json::Value::Array(parts.iter().map(conflict_part_to_json).collect())
}

fn conflict_part_list_from_json(json: &serde_json::Value) -> Vec<ConflictPart> {
    json.as_array()
        .unwrap()
        .iter()
        .map(conflict_part_from_json)
        .collect()
}

fn conflict_part_to_json(part: &ConflictPart) -> serde_json::Value {
    serde_json::json!({
        "value": tree_value_to_json(&part.value),
    })
}

fn conflict_part_from_json(json: &serde_json::Value) -> ConflictPart {
    let json_value = json.get("value").unwrap();
    ConflictPart {
        value: tree_value_from_json(json_value),
    }
}

fn tree_value_to_json(value: &TreeValue) -> serde_json::Value {
    match value {
        TreeValue::Normal { id, executable } => serde_json::json!({
             "file": {
                 "id": id.hex(),
                 "executable": executable,
             },
        }),
        TreeValue::Symlink(id) => serde_json::json!({
             "symlink_id": id.hex(),
        }),
        TreeValue::Tree(id) => serde_json::json!({
             "tree_id": id.hex(),
        }),
        TreeValue::GitSubmodule(id) => serde_json::json!({
             "submodule_id": id.hex(),
        }),
        TreeValue::Conflict(id) => serde_json::json!({
             "conflict_id": id.hex(),
        }),
    }
}

fn tree_value_from_json(json: &serde_json::Value) -> TreeValue {
    if let Some(json_file) = json.get("file") {
        TreeValue::Normal {
            id: FileId::new(bytes_vec_from_json(json_file.get("id").unwrap())),
            executable: json_file.get("executable").unwrap().as_bool().unwrap(),
        }
    } else if let Some(json_id) = json.get("symlink_id") {
        TreeValue::Symlink(SymlinkId::new(bytes_vec_from_json(json_id)))
    } else if let Some(json_id) = json.get("tree_id") {
        TreeValue::Tree(TreeId::new(bytes_vec_from_json(json_id)))
    } else if let Some(json_id) = json.get("submodule_id") {
        TreeValue::GitSubmodule(CommitId::new(bytes_vec_from_json(json_id)))
    } else if let Some(json_id) = json.get("conflict_id") {
        TreeValue::Conflict(ConflictId::new(bytes_vec_from_json(json_id)))
    } else {
        panic!("unexpected json value in conflict: {:#?}", json);
    }
}

fn bytes_vec_from_json(value: &serde_json::Value) -> Vec<u8> {
    hex::decode(value.as_str().unwrap()).unwrap()
}
