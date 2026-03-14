# Changelog

## [1.1.0] - 2026-03-14

### Added
- `manifest.json` for Layer 01 (31,218 files, 55,926 commentaries, 8,681 empty verses flagged)
- `schema.json` — JSON Schema definition for verse commentary files
- `enrichment_config.json` — model, prompts, coverage tracking for Layer 02
- `book_canon.json` — 66 book abbreviation mappings with testament/category/file counts
- `validate_schema.py` — schema validation script with anomaly reporting
- `generate_manifest.py` — manifest generator for any layer
- `gap_audit.py` — layer comparison tool for detecting missing files

### Fixed
- Removed test artifact `test_single_comment.json` from Layer 02
- Removed duplicate `PROVENANCE.json` (kept in `00_raw_archive/` only)

## [1.0.0] - 2026-03-14

### Added
- Initial release with 31,218 verse-level commentary files from CatenaBible.com
- Full Old Testament coverage (23,320 files across 6 categories)
- Full New Testament coverage (7,898 files across 5 categories)
- Translated + AI-enriched layer for Acts (865 files) and John (14 files)
- Theological glossary (23 base EN-PT terms)
- Author metadata for 60+ Church Fathers
- Scraping script (`scrape_catena_bible.py`)
- Translation + enrichment script (`translate_and_enrich.py`)
- PROVENANCE.json with SHA256 integrity checksums
