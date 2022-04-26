# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking changes

* `jj [op] undo` and `jj op restore` used to take the operation to undo or
  restore to as an argument to `-o/--operation`. It is now a positional
  argument instead (i.e. `jj undo -o abc123` is now written `jj undo abc123`).

### New features

* `jj rebase` now accepts a `--branch/-b <revision>` argument, which can be used
  instead of `-r` or `-s` to specify which commits to rebase. It will rebase the
  whole branch, relative to the destination. The default mode has changed from
  `-r @` to `-b @`.

* The new `jj print` command prints the contents of a file in a revision.

* `jj move` and `jj squash` now lets you limit the set of changes to move by
  specifying paths on the command line (in addition to the `--interactive`
  mode). For example, use `jj move --to @-- foo` to move the changes to file
  (or directory) `foo` in the working copy to the grandparent commit.

* `jj split` now lets you specify on the CLI which paths to include in the first
  commit. The interactive diff-editing is not started when you do that.

* Sparse checkouts are now supported. In fact, all working copies are now
  "sparse", only to different degrees. Use the `jj sparse` command to manage
  the paths included in the sparse checkout.  

* The `$JJ_CONFIG` environment variable can now point to a directory. If it
  does, all files in the directory will be read, in alphabetical order.

* You can now override the `$EDITOR` environment variable by setting the
  `ui.editor` config. There is also a new `$JJ_EDITOR` environment variable,
  which has even higher priority than the config.

* The new revset function `connected(x)` is the same as `x:x`.

* The new revset function `roots(x)` finds commits in the set that are not
  descendants of other commits in the set.

### Fixed bugs

* When rebasing a conflict where one side modified a file and the other side
  deleted it, we no longer automatically resolve it in favor of the modified
  content (this was a regression from commit c0ae4b16e8c4).

* Errors are now printed to stderr (they used to be printed to stdout).

* Updating the working copy to a commit where a file's executable bit changed
  but the contents was the same used to lead to a crash. That has now been
  fixed. 

* If one side of a merge modified a directory and the other side deleted it, it
  used to be considered a conflict. The same was true if both sides added a
  directory with different files in. They are now merged as if the missing
  directory had been empty.

* When using `jj move` to move part of a commit into an ancestor, any branches
  pointing to the source commit used to be left on a hidden intermediate commit.
  They are now correctly updated.

* `jj untrack` now requires at least one path (allowing no arguments was a UX
  bug).

* `jj rebase` now requires at least one destination (allowing no arguments was a
  UX bug).

* `jj restore --to <rev>` now restores from the working copy (it used to restore
  from the working copy's parent).

* You now get a proper error message instead of a crash when `$EDITOR` doesn't
  exist or exits with an error.

* Fixed relative path to the current directory in output to be `.` instead of
  empty string.

* When adding a new workspace, the parent of the current workspace's current
  checkout will be checked out. That was always the intent, but the root commit
  was accidentally checked out instead.

## [0.4.0] - 2022-04-02

### Breaking changes

* Dropped support for config in `~/.jjconfig`. Your configuration is now read
  from `<config dir>/jj/config.toml`, where `<config dir>` is
  `${XDG_CONFIG_HOME}` or `~/.config/` on Linux,
  `~/Library/Application Support/` on macOS, and `~\AppData\Roaming\` on
  Windows.

### New features

* You can now set an environment variable called `$JJ_CONFIG` to a path to a
  config file. That will then be read instead of your regular config file. This
  is mostly intended for testing and scripts.

* The [standard `$NO_COLOR` environment variable](https://no-color.org/) is now
  respected.

* `jj new` now lets you specify a description with `--message/-m`.

* When you check out a commit, the old commit no longer automatically gets
  abandoned if it's empty and has descendants, it only gets abandoned if it's
  empty and does not have descendants.

* (#111) When undoing an earlier operation, any new commits on top of commits
  from the undone operation will be rebased away. For example, let's say you
  rebase commit A so it becomes a new commit A', and then you create commit B
  on top of A'. If you now undo the rebase operation, commit B will be rebased
  to be on top of A instead. The same logic is used if the repo was modified
  by concurrent operations (so if one operation added B on top of A, and one
  operation rebased A as A', then B would be automatically rebased on top of
  A'). See #111 for more examples.

* `jj log` now accepts `-p`/`--patch` option.

### Fixed bugs

* Fixed crash on `jj init --git-repo=.` (it almost always crashed).

* When sharing the working copy with a Git repo, the automatic importing and
  exporting (sometimes?) didn't happen on Windows.

## [0.3.3] - 2022-03-16

No changes, only trying to get the automated build to work.

## [0.3.2] - 2022-03-16

No changes, only trying to get the automated build to work.

## [0.3.1] - 2022-03-13

### Fixed bugs

 - (#131) Fixed crash when `core.excludesFile` pointed to non-existent file, and
   made leading `~/` in that config expand to `$HOME/`

## [0.3.0] - 2022-03-12

Last release before this changelog started.
