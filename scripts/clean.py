#!/usr/bin/env python3
"""
clean.py

Cleans verse-level commentary JSON files, producing lean research-ready data.

Uses ftfy (industry standard) for encoding fixes and adds domain-specific
rules for biblical commentary data scraped from CatenaBible.com.

Operations:
  1. Fix text encoding (mojibake, HTML entities) via ftfy
  2. Remove scraping metadata fields from top-level and per-commentary
  3. Normalize verse_text (strip "Verse text for ..." placeholders)
  4. Normalize whitespace (trim, collapse, limit consecutive newlines)
  5. Remove commentaries with empty or trivially short content
  6. Add normalized reference fields (book, chapter, verse) from filename
  7. Recalculate total_commentaries to match actual array length
  8. Unicode NFC normalization

Usage:
    # Dry run (report what would change, don't modify files)
    python scripts/clean.py --dry-run

    # Clean Layer 01 -> 01_cleaned (default)
    python scripts/clean.py

    # Clean specific testament/book
    python scripts/clean.py --testament new_testament --book acts

    # Clean to custom output directory
    python scripts/clean.py --output data/custom_output

    # Verbose (show per-file details)
    python scripts/clean.py --dry-run --verbose

Requirements:
    pip install ftfy
"""

import argparse
import json
import re
import sys
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path

try:
    import ftfy
except ImportError:
    print("Error: ftfy is required. Install with: pip install ftfy")
    sys.exit(1)

REPO_ROOT = Path(__file__).parent.parent
DEFAULT_INPUT = REPO_ROOT / "data" / "00_raw" / "catena_bible"
DEFAULT_OUTPUT = REPO_ROOT / "data" / "01_cleaned" / "catena_bible"

# ============================================================================
# Fields to strip (scraping artifacts, not research content)
# ============================================================================

TOP_LEVEL_REMOVE = {
    "scraped_with",
    "extraction_date",
    "full_content_fetched",
    "methodology",
    "source_url",
}

COMMENTARY_REMOVE = {
    "commentary_number",
    "reading_time",
    "source_url",
    "full_content_url",
    "content_type",
    "extraction_method",
}

# Minimum content length to keep a commentary (shorter is likely artifact)
MIN_CONTENT_LENGTH = 15


# ============================================================================
# Text normalization
# ============================================================================

def normalize_text(text: str) -> str:
    """
    Normalize text using ftfy + additional domain rules.

    Pipeline:
      1. ftfy.fix_text() — fixes mojibake, HTML entities, bad encoding
      2. Unicode NFC normalization
      3. Replace non-breaking spaces with regular spaces
      4. Remove control characters (except newline, tab)
      5. Collapse runs of whitespace (except newlines)
      6. Limit consecutive newlines to 2
      7. Strip leading/trailing whitespace
    """
    if not text:
        return text

    # Step 1: ftfy fixes encoding issues, mojibake, HTML entities
    text = ftfy.fix_text(
        text,
        normalization="NFC",          # Unicode NFC form
        fix_latin_ligatures=True,     # ﬁ -> fi
        fix_character_width=True,     # fullwidth -> normal
        uncurl_quotes=False,          # keep curly quotes (they're valid)
        fix_line_breaks=True,         # normalize line breaks
        remove_terminal_escapes=True, # remove ANSI escape codes
    )

    # Step 2: replace non-breaking spaces
    text = text.replace("\u00a0", " ")
    text = text.replace("\u200b", "")  # zero-width space

    # Step 3: remove control characters (keep \n \t)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)

    # Step 4: collapse multiple spaces (not newlines) into one
    text = re.sub(r"[^\S\n]+", " ", text)

    # Step 5: limit consecutive newlines to 2
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Step 6: fix doubled quotes "" -> "
    text = text.replace('""', '"')

    # Step 7: strip leading punctuation artifacts from truncated scraping
    text = re.sub(r"^[,;:.)\]]+\s*", "", text)

    # Step 8: strip trailing backslashes
    text = text.rstrip("\\")

    # Step 9: strip residual HTML tags
    text = re.sub(r"<[a-z/][^>]*>", "", text, flags=re.IGNORECASE)

    # Step 10: final strip
    text = text.strip()

    return text


def parse_filename(filename: str) -> dict:
    """
    Parse verse filename into structured reference.
    Examples:
        acts_01_01.json  -> {book: 'acts', chapter: 1, verse: 1}
        1cor_13_04.json  -> {book: '1cor', chapter: 13, verse: 4}
        ps_119_176.json  -> {book: 'ps', chapter: 119, verse: 176}
    """
    stem = filename.replace(".json", "")
    match = re.match(r"^(.+?)_(\d+)_(\d+)$", stem)
    if match:
        return {
            "book": match.group(1),
            "chapter": int(match.group(2)),
            "verse": int(match.group(3)),
        }
    return {}


# ============================================================================
# Cleaning logic
# ============================================================================

def clean_verse_text(text: str) -> str:
    """Clean verse_text field, stripping placeholder patterns."""
    if not text:
        return ""
    if text.startswith("Verse text for"):
        return ""
    return normalize_text(text)


def clean_commentary(comm: dict) -> dict | None:
    """
    Clean a single commentary entry.
    Returns None if the commentary should be removed.
    """
    content = comm.get("content", "")
    if not content or not content.strip():
        return None

    cleaned_content = normalize_text(content)

    # Too short after cleaning — likely a scraping artifact
    if len(cleaned_content) < MIN_CONTENT_LENGTH:
        return None

    return {
        "author": normalize_text(comm.get("author") or "Unknown"),
        "period": (comm.get("period") or "").strip(),
        "content": cleaned_content,
    }


def clean_file(data: dict, filename: str) -> dict:
    """Clean a single verse file's data. Returns the cleaned dict."""
    ref = parse_filename(filename)

    verse_text = clean_verse_text(data.get("verse_text", ""))

    raw_commentaries = data.get("commentaries", [])
    cleaned_commentaries = []
    for comm in raw_commentaries:
        cleaned = clean_commentary(comm)
        if cleaned:
            cleaned_commentaries.append(cleaned)

    result = {
        "verse_reference": data.get("verse_reference", ""),
    }

    if ref:
        result["book"] = ref["book"]
        result["chapter"] = ref["chapter"]
        result["verse"] = ref["verse"]

    if verse_text:
        result["verse_text"] = verse_text

    result["commentary_status"] = (
        "available" if cleaned_commentaries else "not_available"
    )
    result["total_commentaries"] = len(cleaned_commentaries)
    result["commentaries"] = cleaned_commentaries

    return result


# ============================================================================
# File processing
# ============================================================================

def process_file(
    input_path: Path,
    output_path: Path | None,
    dry_run: bool,
    verbose: bool,
    stats: Counter,
):
    """Process a single file."""
    try:
        with input_path.open("r", encoding="utf-8") as f:
            original = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        stats["errors"] += 1
        if verbose:
            print(f"  ERROR: {input_path.name}: {e}")
        return

    cleaned = clean_file(original, input_path.name)

    # Track stats
    original_comms = len(original.get("commentaries", []))
    cleaned_comms = len(cleaned["commentaries"])
    removed_comms = original_comms - cleaned_comms
    removed_keys = set(original.keys()) & TOP_LEVEL_REMOVE

    stats["files_processed"] += 1
    stats["commentaries_kept"] += cleaned_comms
    stats["commentaries_removed"] += removed_comms
    stats["fields_removed"] += len(removed_keys)

    if cleaned_comms == 0:
        stats["files_empty"] += 1

    # Detect text normalizations by ftfy
    for comm_orig in original.get("commentaries", []):
        orig_content = comm_orig.get("content", "")
        if orig_content:
            fixed = ftfy.fix_text(orig_content)
            if fixed != orig_content:
                stats["encoding_fixes"] += 1

    has_changes = (
        removed_keys
        or removed_comms > 0
        or original.get("verse_text", "").startswith("Verse text for")
        or any(
            k in comm
            for comm in original.get("commentaries", [])
            for k in COMMENTARY_REMOVE
        )
    )

    if has_changes:
        stats["files_modified"] += 1

    if verbose and has_changes:
        changes = []
        if removed_keys:
            changes.append(f"-{len(removed_keys)} fields")
        if removed_comms:
            changes.append(f"-{removed_comms} empty comms")
        print(f"  {input_path.name}: {', '.join(changes)}")

    if not dry_run and has_changes:
        target = output_path if output_path else input_path
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(cleaned, f, ensure_ascii=False, indent=2)
        tmp.replace(target)


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Clean commentary verse files using ftfy + domain rules",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT,
        help="Input directory (default: data/00_raw/catena_bible)",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help="Output directory (default: data/01_cleaned/catena_bible)",
    )
    parser.add_argument("--testament", help="Filter by testament")
    parser.add_argument("--book", help="Filter by book name")
    parser.add_argument("--dry-run", action="store_true", help="Report only")
    parser.add_argument("--verbose", action="store_true", help="Per-file details")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Input not found: {args.input}")
        sys.exit(1)

    files = sorted(args.input.rglob("*/verses/*.json"))

    if args.testament:
        files = [f for f in files if args.testament.lower() in str(f).lower().replace("\\", "/")]
    if args.book:
        files = [f for f in files if f"/{args.book.lower()}/" in str(f).lower().replace("\\", "/")]

    mode = "DRY RUN" if args.dry_run else f"-> {args.output}"
    print(f"Cleaning {len(files)} files [{mode}]")
    print(f"Engine: ftfy {ftfy.__version__}")

    stats = Counter()
    start = datetime.now()

    for f in files:
        out = args.output / f.relative_to(args.input)
        process_file(f, out, args.dry_run, args.verbose, stats)

    elapsed = (datetime.now() - start).total_seconds()

    print(f"\n{'='*60}")
    print(f"Clean Summary {'(DRY RUN)' if args.dry_run else ''}")
    print(f"{'='*60}")
    print(f"Files processed:        {stats['files_processed']:,}")
    print(f"Files modified:         {stats['files_modified']:,}")
    print(f"Files with 0 comments:  {stats['files_empty']:,}")
    print(f"Commentaries kept:      {stats['commentaries_kept']:,}")
    print(f"Commentaries removed:   {stats['commentaries_removed']:,}")
    print(f"Top-level fields removed: {stats['fields_removed']:,}")
    print(f"Encoding fixes (ftfy):  {stats['encoding_fixes']:,}")
    print(f"Errors:                 {stats['errors']:,}")
    print(f"Time:                   {elapsed:.1f}s")
    print(f"{'='*60}")

    if args.dry_run:
        print("\nNo files were modified. Run without --dry-run to apply.")


if __name__ == "__main__":
    main()
