#!/usr/bin/env python3
"""
push_to_github.py

A safe, dependency-free helper to push your local folder to a GitHub repository.

Usage examples:
    python push_to_github.py --repo https://github.com/OWNER/REPO.git
    python push_to_github.py --repo https://github.com/OWNER/REPO.git -m "Update: docs and pipeline"
    python push_to_github.py --repo https://github.com/OWNER/REPO.git --branch main --create-branch
    python push_to_github.py --repo https://github.com/OWNER/REPO.git --use-token

Authentication:
    Preferred: Login once with the GitHub CLI (`gh auth login`) or Git credential manager.
    Optional: Set env var GITHUB_TOKEN and use --use-token to embed it in the remote URL for this run.
              WARNING: embedding tokens in URLs can leak via shell history or process listings. Use sparingly.

Requirements:
    - git installed and on PATH (`git --version` should work)
    - Run the script from the root of the project you want to push.
"""
from __future__ import annotations

import argparse
import os
import platform
import shlex
import subprocess
import sys
from datetime import datetime
from typing import Optional, Tuple

def run(cmd: str, cwd: Optional[str] = None, check: bool = True) -> Tuple[int, str, str]:
    """Run a shell command and return (rc, stdout, stderr)."""
    proc = subprocess.Popen(
        shlex.split(cmd),
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out, err = proc.communicate()
    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd, output=out, stderr=err)
    return proc.returncode, out.strip(), err.strip()

def git(cmd: str, cwd: Optional[str] = None, check: bool = True) -> Tuple[int, str, str]:
    return run(f"git {cmd}", cwd=cwd, check=check)

def ensure_git_present():
    try:
        _, out, _ = git("--version")
        print(f"[ok] Found {out}")
    except Exception as e:
        sys.exit(f"[fatal] git is not installed or not on PATH. Error: {e}")

def is_git_repo(path: str) -> bool:
    rc, _, _ = git("rev-parse --is-inside-work-tree", cwd=path, check=False)
    return rc == 0

def init_repo(path: str):
    print("[init] Initializing a new git repository…")
    git("init", cwd=path)
    # Default branch: main if supported
    rc, out, _ = git("symbolic-ref --short HEAD", cwd=path, check=False)
    if rc != 0 or not out:
        # Set initial branch to main (git >= 2.28 supports -b on init; otherwise rename)
        try:
            git("checkout -b main", cwd=path)
            print("[init] Created branch 'main'")
        except subprocess.CalledProcessError:
            pass

def get_current_branch(path: str) -> Optional[str]:
    rc, out, _ = git("rev-parse --abbrev-ref HEAD", cwd=path, check=False)
    if rc != 0:
        return None
    return out

def ensure_identity():
    # Make sure user.name and user.email are set
    rc1, name, _ = git("config user.name", check=False)
    rc2, email, _ = git("config user.email", check=False)
    if rc1 != 0 or not name or rc2 != 0 or not email:
        print("[config] git user.name / user.email not set for this repo.")
        suggested_email = os.environ.get("GIT_AUTHOR_EMAIL") or os.environ.get("EMAIL") or "you@example.com"
        suggested_name = os.environ.get("GIT_AUTHOR_NAME") or os.environ.get("USER") or "Your Name"
        # set locally
        git(f'config user.name "{suggested_name}"')
        git(f'config user.email "{suggested_email}"')
        print(f"[config] Set local identity to: {suggested_name} <{suggested_email}>")

def ensure_remote(origin_url: str, use_token: bool) -> str:
    """Ensure remote 'origin' exists and points to origin_url. Optionally embed token."""
    url = origin_url
    if use_token:
        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            sys.exit("[fatal] --use-token provided, but GITHUB_TOKEN is not set in the environment.")
        # transform https://github.com/OWNER/REPO.git to https://<token>@github.com/OWNER/REPO.git
        if not origin_url.startswith("https://"):
            sys.exit("[fatal] --use-token requires an HTTPS GitHub URL (https://github.com/OWNER/REPO.git).")
        url = origin_url.replace("https://", f"https://{token}@", 1)
        print("[auth] Using ephemeral token from GITHUB_TOKEN for this push (not stored)."

)
    # check if origin exists
    rc, out, _ = git("remote get-url origin", check=False)
    if rc == 0:
        if out != url:
            print(f"[remote] Updating existing 'origin' URL.")
            git(f'remote set-url origin "{url}"')
        else:
            print(f"[remote] Remote 'origin' already set.")
    else:
        print(f"[remote] Adding remote 'origin'.")
        git(f'remote add origin "{url}"')
    return url

def set_branch(branch: Optional[str], create_branch: bool, path: str) -> str:
    current = get_current_branch(path) or "main"
    target = branch or current
    if create_branch:
        # Try to create and switch
        rc, _, _ = git(f"rev-parse --verify {shlex.quote(target)}", cwd=path, check=False)
        if rc == 0:
            print(f"[branch] Branch '{target}' already exists. Switching to it.")
            git(f"checkout {shlex.quote(target)}", cwd=path)
        else:
            print(f"[branch] Creating and switching to '{target}'.")
            git(f"checkout -b {shlex.quote(target)}", cwd=path)
    else:
        print(f"[branch] Using branch '{target}'.")
        git(f"checkout {shlex.quote(target)}", cwd=path, check=False)
    return target

def maybe_create_gitignore(path: str):
    gi = os.path.join(path, ".gitignore")
    if os.path.exists(gi):
        return
    print("[init] No .gitignore found. Creating a Python-friendly default.")
    with open(gi, "w", encoding="utf-8") as f:
        f.write(
            "# Byte-compiled / optimized / DLL files\n__pycache__/\n*.py[cod]\n*$py.class\n\n"
            "# Distribution / packaging\nbuild/\ndist/\n*.egg-info/\n\n"
            "# Virtual environments\n.venv/\nvenv/\n.env\n\n"
            "# Editors/IDE\n.vscode/\n.idea/\n\n"
            "# OS\n.DS_Store\nThumbs.db\n"
        )

def stage_and_commit(path: str, message: Optional[str]) -> bool:
    # Stage all changes
    git("add -A", cwd=path)
    # Check if there is anything to commit
    rc, out, _ = git("status --porcelain", cwd=path, check=False)
    if out.strip() == "":
        print("[commit] Nothing to commit (working tree clean).")
        return False
    msg = message or f"Automated push on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} from {platform.node()}"
    git(f'commit -m "{msg}"', cwd=path)
    print(f"[commit] Created commit: {msg}")
    return True

def pull_rebase(path: str, branch: str):
    # Try pulling to avoid non-fast-forward if remote has new commits
    rc, _, err = git(f"pull --rebase origin {shlex.quote(branch)}", cwd=path, check=False)
    if rc != 0:
        print(f"[pull] Skipping rebase pull (no remote branch yet or error): {err}")

def push(path: str, branch: str, set_upstream: bool):
    upstream_flag = "-u " if set_upstream else ""
    print(f"[push] Pushing to origin/{branch}…")
    git(f"push {upstream_flag}origin {shlex.quote(branch)}", cwd=path)
    print("[push] Done.")

def parse_args():
    parser = argparse.ArgumentParser(description="Push your local folder to a GitHub repository.")
    parser.add_argument("--repo", required=True, help="HTTPS URL of the GitHub repository (e.g., https://github.com/OWNER/REPO.git)")
    parser.add_argument("-m", "--message", help="Commit message. Defaults to a timestamped message.")
    parser.add_argument("--branch", help="Branch name to push (defaults to current branch or 'main').")
    parser.add_argument("--create-branch", action="store_true", help="Create the branch if it doesn't exist locally.")
    parser.add_argument("--use-token", action="store_true", help="Use the GITHUB_TOKEN env var to authenticate this push.")
    parser.add_argument("--no-gitignore", action="store_true", help="Do not create a default .gitignore if missing.")
    parser.add_argument("--no-pull", action="store_true", help="Skip 'git pull --rebase' before pushing.")
    return parser.parse_args()

def main():
    args = parse_args()
    root = os.getcwd()

    ensure_git_present()

    if not is_git_repo(root):
        init_repo(root)

    ensure_identity()

    if not args.no_gitignore:
        maybe_create_gitignore(root)

    branch = set_branch(args.branch, args.create_branch, root)

    remote_url = ensure_remote(args.repo, use_token=args.use_token)

    if not args.no_pull:
        pull_rebase(root, branch)

    committed = stage_and_commit(root, args.message)

    # If this is the first push to this branch, set upstream
    rc, _, _ = git("rev-parse --symbolic-full-name --abbrev-ref @{u}", cwd=root, check=False)
    set_upstream = rc != 0
    try:
        push(root, branch, set_upstream=set_upstream)
    finally:
        # If we embedded a token into the remote, reset it to the clean URL after push
        if args.use_token and remote_url != args.repo:
            print("[auth] Resetting 'origin' URL to remove token.")
            git(f'remote set-url origin "{args.repo}"')

    print("\n[success] Pushed successfully!")
    print("Tips:")
    print("  - For 2FA accounts, prefer `gh auth login` or a PAT with 'repo' scope.")
    print("  - To set your global identity: `git config --global user.name 'Your Name'` and `git config --global user.email you@example.com`.")

if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"\n[error] Command failed: {e.cmd}\nExit code: {e.returncode}\nstdout:\n{e.output}\nstderr:\n{e.stderr}")
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        print("\n[cancelled] Interrupted by user.")
        sys.exit(130)
