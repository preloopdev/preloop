2026-08-01T02:36:47.2335951Z Current runner version: '2.336.0'
##[group]Runner Image Provisioner
Hosted Compute Agent
Version: 20260624.560
Commit: 925d229a51159bc391ae97e54a2dd1fe20af789d
Build Date: 2026-06-24T18:26:47Z
Worker ID: {80b4c543-ba0f-4686-a064-20ae2a83361f}
Azure Region: eastus
##[endgroup]
##[group]VM Image
- OS: Linux (x64)
- Source: Docker
- Name: ubuntu:24.04
- Version: 20260629.59.1
##[endgroup]
##[group]GITHUB_TOKEN Permissions
Metadata: read
##[endgroup]
Secret source: None
Prepare workflow directory
Prepare all required actions
Getting action download info
Download action repository 'actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0' (SHA:9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0)
Download action repository 'actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1' (SHA:ece7cb06caefa5fff74198d8649806c4678c61a1)
Uses: astral-sh/uv/.github/workflows/check-lint.yml@refs/pull/20876/merge (81742a27ad62bb1dd1289a164f1776508279c756)
##[group] Inputs
code-changed: false
save-rust-cache: false
##[endgroup]
Complete job name: check-lint / readme
##[group]Run actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0
with:
persist-credentials: false
repository: astral-sh/uv
token: ***
ssh-strict: true
ssh-user: git
clean: true
sparse-checkout-cone-mode: true
fetch-depth: 1
fetch-tags: false
show-progress: true
lfs: false
submodules: false
set-safe-directory: true
allow-unsafe-pr-checkout: false
env:
CARGO_INCREMENTAL: 0
CARGO_NET_RETRY: 10
CARGO_TERM_COLOR: always
HAWK_VERSION: 0.1.9
RUSTUP_MAX_RETRIES: 10
##[endgroup]
Syncing repository: astral-sh/uv
##[group]Getting Git version info
Working directory is '/home/runner/work/uv/uv'
[command]/usr/bin/git version
git version 2.54.0
##[endgroup]
Temporarily overriding HOME='/home/runner/work/_temp/c20f4535-a189-4973-987f-29a2e5dabd07' before making global git config changes
Adding repository directory to the temporary git global config as a safe directory
[command]/usr/bin/git config --global --add safe.directory /home/runner/work/uv/uv
Deleting the contents of '/home/runner/work/uv/uv'
##[group]Determining repository object format
##[endgroup]
##[group]Initializing the repository
[command]/usr/bin/git init /home/runner/work/uv/uv
hint: Using 'master' as the name for the initial branch. This default branch name
hint: will change to "main" in Git 3.0. To configure the initial branch name
hint: to use in all of your new repositories, which will suppress this warning,
hint: call:
hint:
hint: 	git config --global init.defaultBranch <name>
hint:
hint: Names commonly chosen instead of 'master' are 'main', 'trunk' and
hint: 'development'. The just-created branch can be renamed via this command:
hint:
hint: 	git branch -m <name>
hint:
hint: Disable this message with "git config set advice.defaultBranchName false"
Initialized empty Git repository in /home/runner/work/uv/uv/.git/
[command]/usr/bin/git remote add origin https://github.com/astral-sh/uv
##[endgroup]
##[group]Disabling automatic garbage collection
[command]/usr/bin/git config --local gc.auto 0
##[endgroup]
##[group]Setting up auth
Removing SSH command configuration
[command]/usr/bin/git config --local --name-only --get-regexp core\.sshCommand
[command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'core\.sshCommand' && git config --local --unset-all 'core.sshCommand' || :"
Removing HTTP extra header
[command]/usr/bin/git config --local --name-only --get-regexp http\.https\:\/\/github\.com\/\.extraheader
[command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'http\.https\:\/\/github\.com\/\.extraheader' && git config --local --unset-all 'http.https://github.com/.extraheader' || :"
Removing includeIf entries pointing to credentials config files
[command]/usr/bin/git config --local --name-only --get-regexp ^includeIf\.gitdir:
[command]/usr/bin/git submodule foreach --recursive git config --local --show-origin --name-only --get-regexp remote.origin.url
[command]/usr/bin/git config --file /home/runner/work/_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config http.https://github.com/.extraheader AUTHORIZATION: basic ***
[command]/usr/bin/git config --local includeIf.gitdir:/home/runner/work/uv/uv/.git.path /home/runner/work/_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local includeIf.gitdir:/home/runner/work/uv/uv/.git/worktrees/*.path /home/runner/work/_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local includeIf.gitdir:/github/workspace/.git.path /github/runner_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local includeIf.gitdir:/github/workspace/.git/worktrees/*.path /github/runner_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
##[endgroup]
##[group]Fetching the repository
[command]/usr/bin/git -c protocol.version=2 fetch --no-tags --prune --no-recurse-submodules --depth=1 origin +81742a27ad62bb1dd1289a164f1776508279c756:refs/remotes/pull/20876/merge
From https://github.com/astral-sh/uv
* [new ref]         81742a27ad62bb1dd1289a164f1776508279c756 -> pull/20876/merge
##[endgroup]
##[group]Determining the checkout info
##[endgroup]
[command]/usr/bin/git sparse-checkout disable
[command]/usr/bin/git config --local --unset-all extensions.worktreeConfig
##[group]Checking out the ref
[command]/usr/bin/git checkout --progress --force refs/remotes/pull/20876/merge
Note: switching to 'refs/remotes/pull/20876/merge'.
You are in 'detached HEAD' state. You can look around, make experimental
changes and commit them, and you can discard any commits you make in this
state without impacting any branches by switching back to a branch.
If you want to create a new branch to retain commits you create, you may
do so (now or later) by using -c with the switch command. Example:
git switch -c <new-branch-name>
Or undo this operation with:
git switch -
Turn off this advice by setting config variable advice.detachedHead to false
HEAD is now at 81742a2 Merge 1ddf5b709ad49bba88e6607af0d2a1bbe0c03ff5 into 79bbface771210df216b738e9bdc7df95e5a9e6b
##[endgroup]
[command]/usr/bin/git log -1 --format=%H
81742a27ad62bb1dd1289a164f1776508279c756
##[group]Removing auth
Removing SSH command configuration
[command]/usr/bin/git config --local --name-only --get-regexp core\.sshCommand
[command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'core\.sshCommand' && git config --local --unset-all 'core.sshCommand' || :"
Removing HTTP extra header
[command]/usr/bin/git config --local --name-only --get-regexp http\.https\:\/\/github\.com\/\.extraheader
[command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'http\.https\:\/\/github\.com\/\.extraheader' && git config --local --unset-all 'http.https://github.com/.extraheader' || :"
Removing includeIf entries pointing to credentials config files
[command]/usr/bin/git config --local --name-only --get-regexp ^includeIf\.gitdir:
includeif.gitdir:/home/runner/work/uv/uv/.git.path
includeif.gitdir:/home/runner/work/uv/uv/.git/worktrees/*.path
includeif.gitdir:/github/workspace/.git.path
includeif.gitdir:/github/workspace/.git/worktrees/*.path
[command]/usr/bin/git config --local --get-all includeif.gitdir:/home/runner/work/uv/uv/.git.path
/home/runner/work/_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local --unset includeif.gitdir:/home/runner/work/uv/uv/.git.path /home/runner/work/_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local --get-all includeif.gitdir:/home/runner/work/uv/uv/.git/worktrees/*.path
/home/runner/work/_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local --unset includeif.gitdir:/home/runner/work/uv/uv/.git/worktrees/*.path /home/runner/work/_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local --get-all includeif.gitdir:/github/workspace/.git.path
/github/runner_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local --unset includeif.gitdir:/github/workspace/.git.path /github/runner_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local --get-all includeif.gitdir:/github/workspace/.git/worktrees/*.path
/github/runner_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git config --local --unset includeif.gitdir:/github/workspace/.git/worktrees/*.path /github/runner_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config
[command]/usr/bin/git submodule foreach --recursive git config --local --show-origin --name-only --get-regexp remote.origin.url
Removing credentials config '/home/runner/work/_temp/git-credentials-a15bbda4-5296-4a9c-8b53-fbc14181681d.config'
##[endgroup]
##[group]Run actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1
with:
python-version: 3.14
check-latest: false
token: ***
update-environment: true
allow-prereleases: false
freethreaded: false
env:
CARGO_INCREMENTAL: 0
CARGO_NET_RETRY: 10
CARGO_TERM_COLOR: always
HAWK_VERSION: 0.1.9
RUSTUP_MAX_RETRIES: 10
##[endgroup]
##[group]Installed versions
Version 3.14 was not found in the local cache
Version 3.14 is available for downloading
Download from "https://github.com/actions/python-versions/releases/download/3.14.6-27283001424/python-3.14.6-linux-24.04-x64.tar.gz"
Extract downloaded archive
[command]/usr/bin/tar xz --warning=no-unknown-keyword --overwrite -C /home/runner/work/_temp/23b90413-a5d3-44fa-b967-b5d91c523943 -f /home/runner/work/_temp/4b137b08-f283-41bb-9354-160889f2a67d
Execute installation script
Check if Python hostedtoolcache folder exist...
Creating Python hostedtoolcache folder...
Create Python 3.14.6 folder
Copy Python binaries to hostedtoolcache folder
Create additional symlinks (Required for the UsePythonVersion Azure Pipelines task and the setup-python GitHub Action)
Upgrading pip...
Collecting pip
Downloading pip-26.2-py3-none-any.whl.metadata (4.6 kB)
Downloading pip-26.2-py3-none-any.whl (1.8 MB)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1.8/1.8 MB 14.0 MB/s  0:00:00
Installing collected packages: pip
Attempting uninstall: pip
Found existing installation: pip 26.1.2
Uninstalling pip-26.1.2:
Successfully uninstalled pip-26.1.2
Successfully installed pip-26.2
Create complete file
Successfully set up CPython (3.14.6)
##[endgroup]
##[group]Run python scripts/transform_readme.py --target pypi
python scripts/transform_readme.py --target pypi
shell: /usr/bin/bash -e {0}
env:
CARGO_INCREMENTAL: 0
CARGO_NET_RETRY: 10
CARGO_TERM_COLOR: always
HAWK_VERSION: 0.1.9
RUSTUP_MAX_RETRIES: 10
pythonLocation: /opt/hostedtoolcache/Python/3.14.6/x64
PKG_CONFIG_PATH: /opt/hostedtoolcache/Python/3.14.6/x64/lib/pkgconfig
Python_ROOT_DIR: /opt/hostedtoolcache/Python/3.14.6/x64
Python2_ROOT_DIR: /opt/hostedtoolcache/Python/3.14.6/x64
Python3_ROOT_DIR: /opt/hostedtoolcache/Python/3.14.6/x64
LD_LIBRARY_PATH: /opt/hostedtoolcache/Python/3.14.6/x64/lib
##[endgroup]
Post job cleanup.
Post job cleanup.
[command]/usr/bin/git version
git version 2.54.0
Temporarily overriding HOME='/home/runner/work/_temp/194af022-769d-4644-8e68-a88a3e80b50f' before making global git config changes
Adding repository directory to the temporary git global config as a safe directory
[command]/usr/bin/git config --global --add safe.directory /home/runner/work/uv/uv
Removing SSH command configuration
[command]/usr/bin/git config --local --name-only --get-regexp core\.sshCommand
[command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'core\.sshCommand' && git config --local --unset-all 'core.sshCommand' || :"
Removing HTTP extra header
[command]/usr/bin/git config --local --name-only --get-regexp http\.https\:\/\/github\.com\/\.extraheader
[command]/usr/bin/git submodule foreach --recursive sh -c "git config --local --name-only --get-regexp 'http\.https\:\/\/github\.com\/\.extraheader' && git config --local --unset-all 'http.https://github.com/.extraheader' || :"
Removing includeIf entries pointing to credentials config files
[command]/usr/bin/git config --local --name-only --get-regexp ^includeIf\.gitdir:
[command]/usr/bin/git submodule foreach --recursive git config --local --show-origin --name-only --get-regexp remote.origin.url
Cleaning up orphan processes
