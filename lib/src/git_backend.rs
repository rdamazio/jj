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

use std::fmt::{Debug, Error, Formatter};
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use git2::Oid;
use itertools::Itertools;
use protobuf::Message;
use uuid::Uuid;

use crate::backend::{
    Backend, BackendError, BackendResult, ChangeId, Commit, CommitId, Conflict, ConflictId,
    ConflictPart, FileId, MillisSinceEpoch, Signature, SymlinkId, Timestamp, Tree, TreeId,
    TreeValue,
};
use crate::repo_path::{RepoPath, RepoPathComponent};
use crate::stacked_table::{TableSegment, TableStore};

const HASH_LENGTH: usize = 20;
/// Ref namespace used only for preventing GC.
const NO_GC_REF_NAMESPACE: &str = "refs/jj/keep/";
const CONFLICT_SUFFIX: &str = ".jjconflict";

impl From<git2::Error> for BackendError {
    fn from(err: git2::Error) -> Self {
        match err.code() {
            git2::ErrorCode::NotFound => BackendError::NotFound,
            _other => BackendError::Other(err.to_string()),
        }
    }
}

pub struct GitBackend {
    repo: Mutex<git2::Repository>,
    empty_tree_id: TreeId,
    extra_metadata_store: TableStore,
}

impl GitBackend {
    fn new(repo: git2::Repository, extra_metadata_store: TableStore) -> Self {
        let empty_tree_id =
            TreeId::new(hex::decode("4b825dc642cb6eb9a060e54bf8d69288fbee4904").unwrap());
        GitBackend {
            repo: Mutex::new(repo),
            empty_tree_id,
            extra_metadata_store,
        }
    }

    pub fn init_internal(store_path: PathBuf) -> Self {
        let git_repo = git2::Repository::init_bare(&store_path.join("git")).unwrap();
        let extra_path = store_path.join("extra");
        std::fs::create_dir(&extra_path).unwrap();
        let mut git_target_file = File::create(store_path.join("git_target")).unwrap();
        git_target_file.write_all(b"git").unwrap();
        let extra_metadata_store = TableStore::init(extra_path, HASH_LENGTH);
        GitBackend::new(git_repo, extra_metadata_store)
    }

    pub fn init_external(store_path: PathBuf, git_repo_path: PathBuf) -> Self {
        let extra_path = store_path.join("extra");
        std::fs::create_dir(&extra_path).unwrap();
        let mut git_target_file = File::create(store_path.join("git_target")).unwrap();
        git_target_file
            .write_all(git_repo_path.to_str().unwrap().as_bytes())
            .unwrap();
        let repo = git2::Repository::open(store_path.join(git_repo_path)).unwrap();
        let extra_metadata_store = TableStore::init(extra_path, HASH_LENGTH);
        GitBackend::new(repo, extra_metadata_store)
    }

    pub fn load(store_path: PathBuf) -> Self {
        let mut git_target_file = File::open(store_path.join("git_target")).unwrap();
        let mut buf = Vec::new();
        git_target_file.read_to_end(&mut buf).unwrap();
        let git_repo_path_str = String::from_utf8(buf).unwrap();
        let git_repo_path = store_path.join(git_repo_path_str).canonicalize().unwrap();
        let repo = git2::Repository::open(git_repo_path).unwrap();
        let extra_metadata_store = TableStore::load(store_path.join("extra"), HASH_LENGTH);
        GitBackend::new(repo, extra_metadata_store)
    }
}

fn signature_from_git(signature: git2::Signature) -> Signature {
    let name = signature.name().unwrap_or("<no name>").to_owned();
    let email = signature.email().unwrap_or("<no email>").to_owned();
    let timestamp = MillisSinceEpoch((signature.when().seconds() * 1000) as u64);
    let tz_offset = signature.when().offset_minutes();
    Signature {
        name,
        email,
        timestamp: Timestamp {
            timestamp,
            tz_offset,
        },
    }
}

fn signature_to_git(signature: &Signature) -> git2::Signature {
    let name = &signature.name;
    let email = &signature.email;
    let time = git2::Time::new(
        (signature.timestamp.timestamp.0 / 1000) as i64,
        signature.timestamp.tz_offset,
    );
    git2::Signature::new(name, email, &time).unwrap()
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

/// Creates a random ref in refs/jj/. Used for preventing GC of commits we
/// create.
fn create_no_gc_ref() -> String {
    let mut no_gc_ref = NO_GC_REF_NAMESPACE.to_owned();
    let mut uuid_buffer = Uuid::encode_buffer();
    let uuid_str = Uuid::new_v4()
        .to_hyphenated()
        .encode_lower(&mut uuid_buffer);
    no_gc_ref.push_str(uuid_str);
    no_gc_ref
}

impl Debug for GitBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_struct("GitStore")
            .field("path", &self.repo.lock().unwrap().path())
            .finish()
    }
}

impl Backend for GitBackend {
    fn hash_length(&self) -> usize {
        HASH_LENGTH
    }

    fn git_repo(&self) -> Option<git2::Repository> {
        let path = self.repo.lock().unwrap().path().to_owned();
        Some(git2::Repository::open(&path).unwrap())
    }

    fn hg_repo(&self) -> Option<hg::repo::Repo> {
        None
    }

    fn read_file(&self, _path: &RepoPath, id: &FileId) -> BackendResult<Box<dyn Read>> {
        if id.as_bytes().len() != self.hash_length() {
            return Err(BackendError::NotFound);
        }
        let locked_repo = self.repo.lock().unwrap();
        let blob = locked_repo
            .find_blob(Oid::from_bytes(id.as_bytes()).unwrap())
            .unwrap();
        let content = blob.content().to_owned();
        Ok(Box::new(Cursor::new(content)))
    }

    fn write_file(&self, _path: &RepoPath, contents: &mut dyn Read) -> BackendResult<FileId> {
        let mut bytes = Vec::new();
        contents.read_to_end(&mut bytes).unwrap();
        let locked_repo = self.repo.lock().unwrap();
        let oid = locked_repo.blob(&bytes).unwrap();
        Ok(FileId::new(oid.as_bytes().to_vec()))
    }

    fn read_symlink(&self, _path: &RepoPath, id: &SymlinkId) -> Result<String, BackendError> {
        if id.as_bytes().len() != self.hash_length() {
            return Err(BackendError::NotFound);
        }
        let locked_repo = self.repo.lock().unwrap();
        let blob = locked_repo
            .find_blob(Oid::from_bytes(id.as_bytes()).unwrap())
            .unwrap();
        let target = String::from_utf8(blob.content().to_owned()).unwrap();
        Ok(target)
    }

    fn write_symlink(&self, _path: &RepoPath, target: &str) -> Result<SymlinkId, BackendError> {
        let locked_repo = self.repo.lock().unwrap();
        let oid = locked_repo.blob(target.as_bytes()).unwrap();
        Ok(SymlinkId::new(oid.as_bytes().to_vec()))
    }

    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }

    fn read_tree(&self, _path: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        if id == &self.empty_tree_id {
            return Ok(Tree::default());
        }
        if id.as_bytes().len() != self.hash_length() {
            return Err(BackendError::NotFound);
        }

        let locked_repo = self.repo.lock().unwrap();
        let git_tree = locked_repo
            .find_tree(Oid::from_bytes(id.as_bytes()).unwrap())
            .unwrap();
        let mut tree = Tree::default();
        for entry in git_tree.iter() {
            let name = entry.name().unwrap();
            let (name, value) = match entry.kind().unwrap() {
                git2::ObjectType::Tree => {
                    let id = TreeId::from_bytes(entry.id().as_bytes());
                    (entry.name().unwrap(), TreeValue::Tree(id))
                }
                git2::ObjectType::Blob => match entry.filemode() {
                    0o100644 => {
                        let id = FileId::from_bytes(entry.id().as_bytes());
                        if name.ends_with(CONFLICT_SUFFIX) {
                            (
                                &name[0..name.len() - CONFLICT_SUFFIX.len()],
                                TreeValue::Conflict(ConflictId::from_bytes(entry.id().as_bytes())),
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
                    0o100755 => {
                        let id = FileId::from_bytes(entry.id().as_bytes());
                        (
                            name,
                            TreeValue::Normal {
                                id,
                                executable: true,
                            },
                        )
                    }
                    0o120000 => {
                        let id = SymlinkId::from_bytes(entry.id().as_bytes());
                        (name, TreeValue::Symlink(id))
                    }
                    mode => panic!("unexpected file mode {:?}", mode),
                },
                git2::ObjectType::Commit => {
                    let id = CommitId::from_bytes(entry.id().as_bytes());
                    (name, TreeValue::GitSubmodule(id))
                }
                kind => panic!("unexpected object type {:?}", kind),
            };
            tree.set(RepoPathComponent::from(name), value);
        }
        Ok(tree)
    }

    fn write_tree(&self, _path: &RepoPath, contents: &Tree) -> BackendResult<TreeId> {
        let locked_repo = self.repo.lock().unwrap();
        let mut builder = locked_repo.treebuilder(None).unwrap();
        for entry in contents.entries() {
            let name = entry.name().string();
            let (name, id, filemode) = match entry.value() {
                TreeValue::Normal {
                    id,
                    executable: false,
                } => (name, id.as_bytes(), 0o100644),
                TreeValue::Normal {
                    id,
                    executable: true,
                } => (name, id.as_bytes(), 0o100755),
                TreeValue::Symlink(id) => (name, id.as_bytes(), 0o120000),
                TreeValue::Tree(id) => (name, id.as_bytes(), 0o040000),
                TreeValue::GitSubmodule(id) => (name, id.as_bytes(), 0o160000),
                TreeValue::Conflict(id) => (
                    entry.name().string() + CONFLICT_SUFFIX,
                    id.as_bytes(),
                    0o100644,
                ),
            };
            builder
                .insert(name, Oid::from_bytes(id).unwrap(), filemode)
                .unwrap();
        }
        let oid = builder.write().unwrap();
        Ok(TreeId::from_bytes(oid.as_bytes()))
    }

    fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        if id.as_bytes().len() != self.hash_length() {
            return Err(BackendError::NotFound);
        }

        let locked_repo = self.repo.lock().unwrap();
        let git_commit_id = Oid::from_bytes(id.as_bytes())?;
        let commit = locked_repo.find_commit(git_commit_id)?;
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
        let parents = commit
            .parent_ids()
            .map(|oid| CommitId::from_bytes(oid.as_bytes()))
            .collect_vec();
        let tree_id = TreeId::from_bytes(commit.tree_id().as_bytes());
        let description = commit.message().unwrap_or("<no message>").to_owned();
        let author = signature_from_git(commit.author());
        let committer = signature_from_git(commit.committer());

        let mut commit = Commit {
            parents,
            predecessors: vec![],
            root_tree: tree_id,
            change_id,
            description,
            author,
            committer,
            is_open: false,
        };

        let table = self.extra_metadata_store.get_head()?;
        let maybe_extras = table.get_value(git_commit_id.as_bytes());
        if let Some(extras) = maybe_extras {
            deserialize_extras(&mut commit, extras);
        }

        Ok(commit)
    }

    fn write_commit(&self, contents: &Commit) -> BackendResult<CommitId> {
        // TODO: We shouldn't have to create an in-memory index just to write an
        // object...
        let locked_repo = self.repo.lock().unwrap();
        let git_tree = locked_repo.find_tree(Oid::from_bytes(contents.root_tree.as_bytes())?)?;
        let author = signature_to_git(&contents.author);
        let committer = signature_to_git(&contents.committer);
        let message = &contents.description;

        let mut parents = vec![];
        for parent_id in &contents.parents {
            let parent_git_commit =
                locked_repo.find_commit(Oid::from_bytes(parent_id.as_bytes())?)?;
            parents.push(parent_git_commit);
        }
        let parent_refs = parents.iter().collect_vec();
        let git_id = locked_repo.commit(
            Some(&create_no_gc_ref()),
            &author,
            &committer,
            message,
            &git_tree,
            &parent_refs,
        )?;
        let id = CommitId::from_bytes(git_id.as_bytes());
        let extras = serialize_extras(contents);
        let mut mut_table = self
            .extra_metadata_store
            .get_head()
            .unwrap()
            .start_mutation();
        if let Some(existing_extras) = mut_table.get_value(git_id.as_bytes()) {
            if existing_extras != extras {
                return Err(BackendError::Other(format!(
                    "Git commit '{}' already exists with different associated non-Git meta-data",
                    id.hex()
                )));
            }
        }
        mut_table.add_entry(git_id.as_bytes().to_vec(), extras);
        self.extra_metadata_store.save_table(mut_table).unwrap();
        Ok(id)
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

    fn write_conflict(&self, _path: &RepoPath, conflict: &Conflict) -> BackendResult<ConflictId> {
        let json = serde_json::json!({
            "removes": conflict_part_list_to_json(&conflict.removes),
            "adds": conflict_part_list_to_json(&conflict.adds),
        });
        let json_string = json.to_string();
        let bytes = json_string.as_bytes();
        let locked_repo = self.repo.lock().unwrap();
        let oid = locked_repo.blob(bytes).unwrap();
        Ok(ConflictId::from_bytes(oid.as_bytes()))
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::backend::{FileId, MillisSinceEpoch};

    #[test]
    fn read_plain_git_commit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store_path = temp_dir.path().to_path_buf();
        let git_repo_path = temp_dir.path().join("git");
        let git_repo = git2::Repository::init(&git_repo_path).unwrap();

        // Add a commit with some files in
        let blob1 = git_repo.blob(b"content1").unwrap();
        let blob2 = git_repo.blob(b"normal").unwrap();
        let mut dir_tree_builder = git_repo.treebuilder(None).unwrap();
        dir_tree_builder.insert("normal", blob1, 0o100644).unwrap();
        dir_tree_builder.insert("symlink", blob2, 0o120000).unwrap();
        let dir_tree_id = dir_tree_builder.write().unwrap();
        let mut root_tree_builder = git_repo.treebuilder(None).unwrap();
        root_tree_builder
            .insert("dir", dir_tree_id, 0o040000)
            .unwrap();
        let root_tree_id = root_tree_builder.write().unwrap();
        let git_author = git2::Signature::new(
            "git author",
            "git.author@example.com",
            &git2::Time::new(1000, 60),
        )
        .unwrap();
        let git_committer = git2::Signature::new(
            "git committer",
            "git.committer@example.com",
            &git2::Time::new(2000, -480),
        )
        .unwrap();
        let git_tree = git_repo.find_tree(root_tree_id).unwrap();
        let git_commit_id = git_repo
            .commit(
                None,
                &git_author,
                &git_committer,
                "git commit message",
                &git_tree,
                &[],
            )
            .unwrap();
        let commit_id = CommitId::from_hex("efdcea5ca4b3658149f899ca7feee6876d077263");
        // The change id is the leading reverse bits of the commit id
        let change_id = ChangeId::from_hex("c64ee0b6e16777fe53991f9281a6cd25");
        // Check that the git commit above got the hash we expect
        assert_eq!(git_commit_id.as_bytes(), commit_id.as_bytes());

        let store = GitBackend::init_external(store_path, git_repo_path);
        let commit = store.read_commit(&commit_id).unwrap();
        assert_eq!(&commit.change_id, &change_id);
        assert_eq!(commit.parents, vec![]);
        assert_eq!(commit.predecessors, vec![]);
        assert_eq!(commit.root_tree.as_bytes(), root_tree_id.as_bytes());
        assert!(!commit.is_open);
        assert_eq!(commit.description, "git commit message");
        assert_eq!(commit.author.name, "git author");
        assert_eq!(commit.author.email, "git.author@example.com");
        assert_eq!(
            commit.author.timestamp.timestamp,
            MillisSinceEpoch(1000 * 1000)
        );
        assert_eq!(commit.author.timestamp.tz_offset, 60);
        assert_eq!(commit.committer.name, "git committer");
        assert_eq!(commit.committer.email, "git.committer@example.com");
        assert_eq!(
            commit.committer.timestamp.timestamp,
            MillisSinceEpoch(2000 * 1000)
        );
        assert_eq!(commit.committer.timestamp.tz_offset, -480);

        let root_tree = store
            .read_tree(
                &RepoPath::root(),
                &TreeId::from_bytes(root_tree_id.as_bytes()),
            )
            .unwrap();
        let mut root_entries = root_tree.entries();
        let dir = root_entries.next().unwrap();
        assert_eq!(root_entries.next(), None);
        assert_eq!(dir.name().as_str(), "dir");
        assert_eq!(
            dir.value(),
            &TreeValue::Tree(TreeId::from_bytes(dir_tree_id.as_bytes()))
        );

        let dir_tree = store
            .read_tree(
                &RepoPath::from_internal_string("dir"),
                &TreeId::from_bytes(dir_tree_id.as_bytes()),
            )
            .unwrap();
        let mut files = dir_tree.entries();
        let normal_file = files.next().unwrap();
        let symlink = files.next().unwrap();
        assert_eq!(files.next(), None);
        assert_eq!(normal_file.name().as_str(), "normal");
        assert_eq!(
            normal_file.value(),
            &TreeValue::Normal {
                id: FileId::from_bytes(blob1.as_bytes()),
                executable: false
            }
        );
        assert_eq!(symlink.name().as_str(), "symlink");
        assert_eq!(
            symlink.value(),
            &TreeValue::Symlink(SymlinkId::from_bytes(blob2.as_bytes()))
        );
    }

    #[test]
    fn commit_has_ref() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = GitBackend::init_internal(temp_dir.path().to_path_buf());
        let signature = Signature {
            name: "Someone".to_string(),
            email: "someone@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let commit = Commit {
            parents: vec![],
            predecessors: vec![],
            root_tree: store.empty_tree_id().clone(),
            change_id: ChangeId::new(vec![]),
            description: "initial".to_string(),
            author: signature.clone(),
            committer: signature,
            is_open: false,
        };
        let commit_id = store.write_commit(&commit).unwrap();
        let git_refs = store
            .git_repo()
            .unwrap()
            .references_glob("refs/jj/keep/*")
            .unwrap()
            .map(|git_ref| git_ref.unwrap().target().unwrap())
            .collect_vec();
        assert_eq!(
            git_refs,
            vec![Oid::from_bytes(commit_id.as_bytes()).unwrap()]
        );
    }

    #[test]
    fn overlapping_git_commit_id() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = GitBackend::init_internal(temp_dir.path().to_path_buf());
        let signature = Signature {
            name: "Someone".to_string(),
            email: "someone@example.com".to_string(),
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(0),
                tz_offset: 0,
            },
        };
        let commit1 = Commit {
            parents: vec![],
            predecessors: vec![],
            root_tree: store.empty_tree_id().clone(),
            change_id: ChangeId::new(vec![]),
            description: "initial".to_string(),
            author: signature.clone(),
            committer: signature,
            is_open: false,
        };
        let commit_id1 = store.write_commit(&commit1).unwrap();
        let mut commit2 = commit1;
        commit2.predecessors.push(commit_id1.clone());
        let expected_error_message = format!("Git commit '{}' already exists", commit_id1.hex());
        match store.write_commit(&commit2) {
            Ok(_) => {
                panic!("expectedly successfully wrote two commits with the same git commit object")
            }
            Err(BackendError::Other(message)) if message.contains(&expected_error_message) => {}
            Err(err) => panic!("unexpected error: {:?}", err),
        };
    }
}
