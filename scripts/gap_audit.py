#!/usr/bin/env python3
"""
gap_audit.py

Compares two commentary layers to find missing files.
Primary use: compare 00_raw_archive vs 01_original to detect gaps.

Usage:
    python scripts/gap_audit.py --source data/00_raw_archive/catena_bible --target data/01_original/catena_bible
"""

import argparse
from pathlib import Path


def audit(source_dir: Path, target_dir: Path):
    source_files = set()
    for f in source_dir.rglob("*/verses/*.json"):
        source_files.add(f.relative_to(source_dir))

    target_files = set()
    for f in target_dir.rglob("*/verses/*.json"):
        target_files.add(f.relative_to(target_dir))

    missing_in_target = source_files - target_files
    extra_in_target = target_files - source_files
    common = source_files & target_files

    print(f"Source: {len(source_files)} verse files")
    print(f"Target: {len(target_files)} verse files")
    print(f"Common: {len(common)}")
    print(f"Missing in target: {len(missing_in_target)}")
    print(f"Extra in target: {len(extra_in_target)}")

    if missing_in_target:
        print(f"\nMissing files (first 20):")
        for f in sorted(missing_in_target)[:20]:
            print(f"  {f}")

    if extra_in_target:
        print(f"\nExtra files in target (first 20):")
        for f in sorted(extra_in_target)[:20]:
            print(f"  {f}")

    return missing_in_target, extra_in_target


def main():
    parser = argparse.ArgumentParser(description="Audit gaps between layers")
    parser.add_argument("--source", required=True, help="Source directory (e.g., 00_raw_archive)")
    parser.add_argument("--target", required=True, help="Target directory (e.g., 01_original)")
    args = parser.parse_args()

    audit(Path(args.source), Path(args.target))


if __name__ == "__main__":
    main()
