# Methodology

How the Bible Commentaries Dataset is built, maintained, and extended.

## Pipeline Overview

```
                    ┌─────────────────────────────────────────────────┐
                    │              SOURCES                             │
                    │  CatenaBible.com, Aquinas Catena, future...      │
                    └──────────────────┬──────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STEP 0: SCRAPE                                                      │
│  Script: scripts/scrape_catena_bible.py                              │
│  Method: Hybrid Firecrawl + BeautifulSoup                            │
│  Output: data/00_raw/catena_bible/                                   │
│                                                                      │
│  One JSON file per verse with all available commentaries.            │
│  Includes scraping metadata (extraction_date, source_url, etc.)     │
│  This layer is NEVER modified after scraping.                        │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STEP 1: CLEAN                                                       │
│  Script: scripts/clean.py                                            │
│  Engine: ftfy (encoding fixes) + domain rules                        │
│  Input:  data/00_raw/catena_bible/                                   │
│  Output: data/01_cleaned/catena_bible/                               │
│                                                                      │
│  Operations:                                                         │
│  - Fix mojibake and encoding errors (ftfy)                          │
│  - Strip scraping metadata (source_url, extraction_date, etc.)      │
│  - Strip per-commentary noise (reading_time, content_type, etc.)    │
│  - Remove verse_text placeholders                                    │
│  - Fix doubled quotes, leading punctuation, trailing backslashes    │
│  - Add parsed reference fields (book, chapter, verse)               │
│  - Remove empty commentaries                                        │
│  - Unicode NFC normalization                                         │
│                                                                      │
│  Result: lean JSON with only research-relevant fields.              │
│  Size reduction: ~33% smaller than raw.                             │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STEP 2: TRANSLATE                                                   │
│  Script: scripts/translate.py --lang pt                              │
│  Model:  GPT-4o-mini (configurable)                                  │
│  Input:  data/01_cleaned/catena_bible/                               │
│  Output: data/02_translated/pt/                                      │
│                                                                      │
│  Adds content_pt for each commentary.                               │
│  Uses theological glossary for consistency.                          │
│  Cheap (~$0.001/file). Run on entire corpus first.                  │
│                                                                      │
│  Output format (lean):                                               │
│  { verse_reference, commentaries: [{author, period, content_pt}] }  │
│  Separate translation_metadata.json per language.                    │
│                                                                      │
│  Multi-language ready: --lang es, --lang fr, etc.                   │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  STEP 3: ENRICH (selective)                                          │
│  Script: scripts/enrich.py --lang pt                                 │
│  Model:  GPT-4o-mini or GPT-4o (configurable)                       │
│  Input:  data/02_translated/pt/                                      │
│  Output: data/03_enriched/pt/                                        │
│                                                                      │
│  Adds structured AI analysis per commentary:                         │
│  - ai_summary (one_sentence, abstract, key_points)                  │
│  - argumentative_structure (thesis, arguments, objections)           │
│  - theological_analysis (doctrines, traditions, method)              │
│  - spiritual_insight (theme, practical_reflection)                   │
│                                                                      │
│  Costly (~$0.01/file). Run selectively on books needed              │
│  for research, not necessarily on entire corpus.                     │
└──────────────────────────────────────────────────────────────────────┘
```

## Data Flow Diagram

```
Source Website
     │
     ▼
 ┌────────┐     ┌────────┐     ┌──────────────┐     ┌────────────┐
 │ 00_raw │────▶│01_clean│────▶│02_translated/│────▶│03_enriched/│
 │ 31,218 │     │ 31,218 │     │   pt/ (861)  │     │  pt/ (879) │
 │  195MB │     │  131MB │     │              │     │            │
 └────────┘     └────────┘     │   es/ (future│     │  es/ (future)
  IMMUTABLE      ftfy+rules    │   fr/ (future│     │  fr/ (future)
                               └──────────────┘     └────────────┘
                                    CHEAP               SELECTIVE
```

## Layer Guarantees

| Layer | Immutable? | Idempotent? | Depends on |
|-------|:----------:|:-----------:|------------|
| 00_raw | Yes — never modified | N/A | Source website |
| 01_cleaned | Regenerable from 00 | Yes — same input = same output | 00_raw + clean.py |
| 02_translated | Regenerable from 01 | Yes — skips already-translated | 01_cleaned + translate.py |
| 03_enriched | Regenerable from 02 | Yes — skips already-enriched | 02_translated + enrich.py |

## Quality Checks

| Check | Script | When to run |
|-------|--------|-------------|
| Schema validation | `scripts/validate_schema.py` | After scraping or cleaning |
| Layer comparison | `scripts/gap_audit.py` | After adding new books |
| Inventory | `scripts/generate_manifest.py` | After any layer changes |

```bash
# Validate raw data schema
python scripts/validate_schema.py --data-dir data/00_raw/catena_bible

# Compare layers for missing files
python scripts/gap_audit.py --source data/00_raw/catena_bible --target data/01_cleaned/catena_bible

# Generate manifest for a layer
python scripts/generate_manifest.py --layer 01_cleaned
```

## File Naming Convention

```
{book_abbreviation}_{chapter:02d}_{verse:02d}.json

Examples:
  acts_01_01.json      Acts 1:1
  1cor_13_04.json      1 Corinthians 13:4
  ps_119_176.json      Psalms 119:176
  gn_01_01.json        Genesis 1:1
```

## Directory Structure Convention

```
data/{layer}/{source_or_lang}/{testament}/{category}/{book}/verses/{file}.json
```

For 00_raw and 01_cleaned:
```
data/01_cleaned/catena_bible/new_testament/gospels/john/verses/jn_01_01.json
data/01_cleaned/catena_bible/old_testament/pentateuch/gn/verses/gn_01_01.json
```

For 02_translated and 03_enriched:
```
data/02_translated/pt/new_testament/acts/acts/verses/acts_01_01.json
data/03_enriched/pt/new_testament/acts/acts/verses/acts_01_01.json
```

## Provenance Tracking

Each pipeline run is tracked:

- **00_raw**: `PROVENANCE.json` at `data/` level with SHA256 checksums of sampled files
- **01_cleaned**: Deterministic from 00_raw + clean.py version
- **02_translated**: `translation_metadata.json` per language with model, date range, file count
- **03_enriched**: `enrichment_config.json` per language with model, fields generated, coverage stats
