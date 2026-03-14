#!/usr/bin/env python3
"""
validate_schema.py

Validates all verse JSON files in 01_original against the expected schema.
Reports anomalies: missing fields, empty content, count mismatches, placeholders.

Usage:
    python scripts/validate_schema.py
    python scripts/validate_schema.py --limit 100
"""

import argparse
import json
from collections import Counter
from pathlib import Path


def validate_file(filepath: Path) -> list[dict]:
    """Validate a single verse file and return list of issues."""
    issues = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return [{"type": "parse_error", "detail": str(e)}]

    # Check required fields
    if "verse_reference" not in data:
        issues.append({"type": "missing_field", "detail": "verse_reference"})
    if "commentaries" not in data:
        issues.append({"type": "missing_field", "detail": "commentaries"})
        return issues

    # Check verse_text placeholder
    verse_text = data.get("verse_text", "")
    if verse_text.startswith("Verse text for"):
        issues.append({"type": "placeholder_verse_text", "detail": verse_text[:60]})

    # Check commentary count mismatch
    declared = data.get("total_commentaries", 0)
    actual = len(data.get("commentaries", []))
    if declared != actual:
        issues.append({
            "type": "count_mismatch",
            "detail": f"declared={declared} actual={actual}",
        })

    # Check empty commentaries array
    if actual == 0:
        issues.append({"type": "no_commentaries", "detail": ""})
        return issues

    # Check individual commentaries
    for i, comm in enumerate(data.get("commentaries", [])):
        if not comm.get("content"):
            issues.append({
                "type": "empty_content",
                "detail": f"commentary[{i}] by {comm.get('author', '?')}",
            })
        if not comm.get("author"):
            issues.append({"type": "missing_author", "detail": f"commentary[{i}]"})

    return issues


def main():
    parser = argparse.ArgumentParser(description="Validate commentary schema")
    parser.add_argument("--limit", type=int, default=0, help="Limit files to validate")
    parser.add_argument(
        "--data-dir",
        default="data/01_original/catena_bible",
        help="Path to verse files",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    files = sorted(data_dir.rglob("*/verses/*.json"))

    if args.limit:
        files = files[: args.limit]

    print(f"Validating {len(files)} files...")

    issue_counts = Counter()
    files_with_issues = 0
    total_issues = 0
    sample_issues = []

    for f in files:
        issues = validate_file(f)
        if issues:
            files_with_issues += 1
            total_issues += len(issues)
            for issue in issues:
                issue_counts[issue["type"]] += 1
                if len(sample_issues) < 20:
                    sample_issues.append(
                        {"file": str(f.relative_to(data_dir)), **issue}
                    )

    print(f"\nResults:")
    print(f"  Total files: {len(files)}")
    print(f"  Files with issues: {files_with_issues}")
    print(f"  Clean files: {len(files) - files_with_issues}")
    print(f"  Total issues: {total_issues}")
    print(f"\nIssue breakdown:")
    for issue_type, count in issue_counts.most_common():
        print(f"  {issue_type}: {count}")

    if sample_issues:
        print(f"\nSample issues (first 20):")
        for s in sample_issues:
            print(f"  [{s['type']}] {s.get('detail', '')} — {s['file']}")


if __name__ == "__main__":
    main()
