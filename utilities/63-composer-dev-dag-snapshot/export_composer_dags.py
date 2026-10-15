#!/usr/bin/env python3
"""Copy a Composer DAG prefix to a backup bucket and write a path manifest.

The manifest lists relative file paths only. It does not read Airflow
variables, connections, or environment values.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="gs://bucket/dags")
    parser.add_argument("--dest", required=True, help="gs://backup-bucket/prefix")
    args = parser.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = args.dest.rstrip("/") + "/" + stamp
    source = args.source.rstrip("/")
    run(["gcloud", "storage", "cp", "-r", source, dest + "/dags"])
    listing = run(["gcloud", "storage", "ls", "-r", dest + "/dags"])
    paths = [
        line.strip()
        for line in listing.splitlines()
        if line.strip() and not line.endswith("/")
    ]
    manifest = {
        "copied_at": stamp,
        "file_count": len(paths),
        "paths": paths,
    }
    manifest_path = "/tmp/composer-dag-manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    run(["gcloud", "storage", "cp", manifest_path, dest + "/manifest.json"])
    print(f"copied {len(paths)} files to {dest}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr or str(exc))
        raise SystemExit(exc.returncode) from exc
