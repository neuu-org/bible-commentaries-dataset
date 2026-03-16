#!/usr/bin/env python3
"""
generate_manifest.py

Generates manifest.json for a commentary data layer.
Counts files, commentaries, coverage by book, and flags anomalies.

Usage:
    python scripts/generate_manifest.py --layer 01_original
    python scripts/generate_manifest.py --layer 01_cleaned
    python scripts/generate_manifest.py --layer 02_translated
    python scripts/generate_manifest.py --layer 03_enriched
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def generate(layer_dir: Path, layer_name: str) -> dict:
    entries = []
    by_book = defaultdict(int)
    total_commentaries = 0
    anomalies = []

    for f in sorted(layer_dir.rglob("*/verses/*.json")):
        rel = str(f.relative_to(layer_dir))
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)

            verse_ref = data.get("verse_reference", "")
            n_comments = len(data.get("commentaries", []))
            status = data.get("commentary_status", "")
            book = f.stem.rsplit("_", 2)[0]

            by_book[book] += 1
            total_commentaries += n_comments

            entry = {
                "path": rel,
                "verse_reference": verse_ref,
                "commentaries_count": n_comments,
                "status": status,
            }

            # Check for enrichment fields (translated/enriched layers)
            if layer_name in ("02_translated", "03_enriched"):
                comms = data.get("commentaries", [])
                has_pt = any(c.get("content_pt") for c in comms)
                has_enrichment = any(c.get("ai_summary") for c in comms)
                entry["has_translation"] = has_pt
                entry["has_enrichment"] = has_enrichment

            entries.append(entry)

            if n_comments == 0:
                anomalies.append({"file": rel, "issue": "no commentaries"})

        except Exception as e:
            anomalies.append({"file": rel, "issue": str(e)})

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "layer": layer_name,
        "total_verse_files": len(entries),
        "total_individual_commentaries": total_commentaries,
        "books": dict(sorted(by_book.items())),
        "anomalies_found": len(anomalies),
        "anomalies_sample": anomalies[:20],
        "entries": entries,
    }

    return manifest


def main():
    parser = argparse.ArgumentParser(description="Generate layer manifest")
    parser.add_argument(
        "--layer",
        required=True,
        choices=["00_raw", "01_original", "01_cleaned", "02_translated", "03_enriched"],
        help="Which layer to process",
    )
    args = parser.parse_args()

    layer_dir = Path(f"data/{args.layer}")
    search_dir = layer_dir / "catena_bible" if "catena_bible" in str(list(layer_dir.rglob("*.json"))[:1]) else layer_dir

    manifest = generate(search_dir, args.layer)

    output_path = layer_dir / "manifest.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"Manifest written to {output_path}")
    print(f"  Files: {manifest['total_verse_files']}")
    print(f"  Commentaries: {manifest['total_individual_commentaries']}")
    print(f"  Books: {len(manifest['books'])}")
    print(f"  Anomalies: {manifest['anomalies_found']}")


if __name__ == "__main__":
    main()
