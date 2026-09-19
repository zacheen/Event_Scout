"""Keep cloud state in a separately verified private GitHub repository."""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / ".ledger"
BRANCH = "data"
FILES = ("cloud_data/events.jsonl", "cloud_data/score_cache.jsonl")
# The run log goes to the SAME private repository rather than the Actions log,
# which is public on a public repository, and rather than nowhere. Discarding it
# was the earlier behaviour and it made a cloud failure undiagnosable, which is
# the one outcome this project treats as worse than a crash.
LOG = "cloud_logs/scan.log"
SCAN_LOG = ROOT / ".private" / "scan.log"


def validate_repository(repository: str, token: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) or not token:
        raise ValueError("Private ledger configuration is missing or invalid")
    request = Request(
        f"https://api.github.com/repos/{repository}",
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json",
                 "User-Agent": "event-scout"})
    with urlopen(request, timeout=30) as response:
        metadata = json.load(response)
    if (metadata.get("private") is not True
            or str(metadata.get("full_name", "")).lower() != repository.lower()):
        raise ValueError("Ledger repository must be private")
    if repository.lower() == os.getenv("GITHUB_REPOSITORY", "").lower():
        raise ValueError("Ledger repository must be separate from the code repository")
    return f"https://github.com/{repository}.git"


def _git(args: list[str], token: str, *, cwd: Path = ROOT,
         allowed: tuple[int, ...] = (0,)) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    auth = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    env.update({"GIT_TERMINAL_PROMPT": "0", "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "http.https://github.com/.extraheader",
                "GIT_CONFIG_VALUE_0": f"AUTHORIZATION: basic {auth}"})
    # Capture output so neither private repository names nor credentials enter CI logs.
    result = subprocess.run(["git", *args], cwd=cwd, env=env,
                            capture_output=True, text=True, timeout=60,
                            encoding="utf-8", errors="replace")
    if result.returncode not in allowed:
        raise RuntimeError("Private ledger Git operation failed")
    return result


def prepare(remote: str, token: str) -> None:
    if LEDGER.exists():
        raise ValueError("Ledger checkout already exists")
    exists = _git(["ls-remote", "--exit-code", "--heads", remote, BRANCH],
                  token, allowed=(0, 2)).returncode == 0
    if exists:
        _git(["clone", "--depth", "2", "--branch", BRANCH, remote, str(LEDGER)], token)
    else:
        _git(["init", "-b", BRANCH, str(LEDGER)], token)
        _git(["remote", "add", "origin", remote], token, cwd=LEDGER)


def _stage_log() -> None:
    """Copy this run's log into the checkout, if the scan produced one.

    Best effort on purpose. A log that cannot be copied must never be the thing
    that fails a run whose ledger is otherwise ready to push.
    """
    if not SCAN_LOG.is_file():
        return
    target = LEDGER / LOG
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(SCAN_LOG.read_bytes())
    except OSError:
        pass


def _commit(token: str) -> None:
    _stage_log()
    # Staged separately. `git add` fails the WHOLE invocation on a pathspec that
    # matches nothing, so pairing the log with the ledger would mean a run that
    # produced no log also committed no ledger.
    for path in FILES:
        if (LEDGER / path).exists():
            _git(["add", "--", path], token, cwd=LEDGER)
    if (LEDGER / LOG).exists():
        _git(["add", "--", LOG], token, cwd=LEDGER)
    changed = _git(["diff", "--cached", "--quiet"], token,
                   cwd=LEDGER, allowed=(0, 1)).returncode == 1
    if changed:
        _git(["commit", "-m", "update private event ledger [skip ci]"], token, cwd=LEDGER)


def save(remote: str, token: str) -> None:
    from .store import SqliteEventStore

    if not LEDGER.resolve().is_relative_to(ROOT.resolve()):
        raise ValueError("Ledger checkout must stay inside the project")
    actual = _git(["remote", "get-url", "origin"], token, cwd=LEDGER).stdout.strip()
    if actual != remote:
        raise ValueError("Ledger remote differs from the verified private repository")
    have_ledger = any((LEDGER / name).exists() for name in FILES)
    if have_ledger and not all((LEDGER / name).is_file() for name in FILES):
        raise ValueError("Ledger export is incomplete")
    # A scan that died before exporting still has a log worth keeping, and that
    # is exactly the run someone needs to read.
    if not have_ledger and not SCAN_LOG.is_file():
        return
    _git(["config", "user.name", "github-actions[bot]"], token, cwd=LEDGER)
    _git(["config", "user.email", "github-actions[bot]@users.noreply.github.com"],
         token, cwd=LEDGER)
    _commit(token)
    private = ROOT / ".private"
    private.mkdir(exist_ok=True)
    for attempt in range(3):
        pushed = _git(["push", remote, f"HEAD:refs/heads/{BRANCH}"], token,
                      cwd=LEDGER, allowed=(0, 1)).returncode == 0
        if pushed:
            return
        if attempt == 2:
            raise RuntimeError("Private ledger push failed")
        # Preserve sticky delivery timestamps if another writer advanced the branch.
        with tempfile.TemporaryDirectory(dir=private) as tmp:
            ours = [Path(tmp) / Path(name).name for name in FILES]
            for source, target in zip(FILES, ours):
                target.write_bytes((LEDGER / source).read_bytes())
            _git(["fetch", "--depth", "2", remote, BRANCH], token, cwd=LEDGER)
            _git(["reset", "--hard", "FETCH_HEAD"], token, cwd=LEDGER)
            merged = SqliteEventStore(":memory:")
            try:
                merged.import_jsonl(*(LEDGER / name for name in FILES))
                merged.import_jsonl(*ours)
                merged.export_jsonl(*(LEDGER / name for name in FILES))
            finally:
                merged.close()
        _commit(token)


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    try:
        if args not in (["prepare"], ["save"]):
            raise ValueError("Expected prepare or save")
        token = os.getenv("LEDGER_TOKEN", "")
        remote = validate_repository(os.getenv("LEDGER_REPOSITORY", ""), token)
        (prepare if args[0] == "prepare" else save)(remote, token)
    except Exception:
        print("::error::Private ledger operation failed. Check private storage and token permissions.")
        return 1
    print("Private ledger operation completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
