# Bible Commentaries Dataset

Curated, multi-layered dataset of patristic and historical biblical commentaries spanning **AD 100-1700**, organized for reproducible research in NLP, information retrieval, and theological analysis.

Part of the [NEUU](https://github.com/neuu-org) biblical scholarship ecosystem.

## Overview

This dataset provides verse-level commentaries from the Church Fathers and historical theologians, structured in progressive layers from raw extraction through AI-enriched analysis.

### Current Scale

| Metric | Value |
|--------|-------|
| Verse files | 31,218 |
| Individual commentaries | 55,925 |
| Old Testament | 23,320 verses across 6 categories |
| New Testament | 7,898 verses across 5 categories |
| Unique authors | 100+ Church Fathers and theologians |
| Time span | ~1,600 years of exegetical tradition |

## Pipeline

Each numbered directory is one step. Each script maps exactly one transition.

```
00_raw  --[clean.py]-->  01_cleaned  --[translate.py --lang pt]-->  02_translated/pt  --[enrich.py --lang pt]-->  03_enriched/pt
```

| Step | Script | Input | Output | Cost |
|------|--------|-------|--------|------|
| 0 | `clean.py` | 00_raw | 01_cleaned | Free (ftfy) |
| 1 | `translate.py` | 01_cleaned | 02_translated | ~$0.001/file (GPT-4o-mini) |
| 2 | `enrich.py` | 02_translated | 03_enriched | ~$0.01/file (GPT-4o-mini) |

## Repository Structure

```
bible-commentaries-dataset/
├── data/
│   ├── PROVENANCE.json              # SHA256 checksums and source metadata
│   │
│   ├── 00_raw/                      # Raw scraped verse files (untouched)
│   │   ├── catena_bible/
│   │   │   ├── old_testament/       # 23,320 files
│   │   │   └── new_testament/       # 7,898 files
│   │   ├── manifest.json            # Full inventory with commentary counts
│   │   └── schema.json              # JSON Schema for verse files
│   │
│   ├── 01_cleaned/                  # Cleaned via ftfy (encoding fixed, metadata stripped)
│   │   └── catena_bible/            # 31,218 lean JSON files
│   │
│   ├── 02_translated/              # Translations by language
│   │   └── pt/                     # Portuguese (BR) — 861 files
│   │       └── new_testament/...
│   │   └── (future: es/, fr/, etc.)
│   │
│   └── 03_enriched/                # AI structured analysis by language
│       └── pt/                     # Portuguese (BR) — 879 files
│           └── new_testament/...
│       └── (future: es/, fr/, etc.)
│
├── scripts/
│   ├── scrape_catena_bible.py       # How the raw data was extracted
│   ├── clean.py                     # 00_raw -> 01_cleaned
│   ├── translate.py                 # 01_cleaned -> 02_translated
│   ├── enrich.py                    # 02_translated -> 03_enriched
│   ├── validate_schema.py           # Validate verse files against schema
│   ├── generate_manifest.py         # Generate manifest.json for any layer
│   └── gap_audit.py                 # Compare two layers to find missing files
│
├── requirements.txt
├── CHANGELOG.md
└── LICENSE                          # CC BY 4.0
```

## Data Layers

### 00_raw — Scraped Data

Verse files exactly as extracted from CatenaBible.com. Contains scraping metadata (extraction_date, source_url, methodology, etc.). Never modified.

### 01_cleaned — Research-Ready

Cleaned via [ftfy](https://github.com/rspeer/python-ftfy): encoding fixes (14,906 corrections), scraping metadata stripped, verse text placeholders removed, normalized reference fields added. **33% smaller** than raw.

```json
{
  "verse_reference": "ACTS 1:1",
  "book": "acts",
  "chapter": 1,
  "verse": 1,
  "commentary_status": "available",
  "total_commentaries": 12,
  "commentaries": [
    {
      "author": "Bede",
      "period": "AD735",
      "content": "Full commentary text..."
    }
  ]
}
```

### 02_translated — Portuguese Translation

Same structure as 01_cleaned, with `content_pt` and `translation_metadata` added per commentary. Cheap to generate (~$0.001/file with GPT-4o-mini).

### 03_enriched — AI Structured Analysis

Same structure as 02_translated, with additional fields per commentary:

| Field | Description |
|-------|-------------|
| `ai_summary` | one_sentence, abstract, key_points |
| `argumentative_structure` | thesis, arguments, objections, conclusion |
| `theological_analysis` | doctrines, traditions, church_fathers, method, controversies |
| `spiritual_insight` | theme, practical_reflection |
| `enrichment_metadata` | model, timestamp, status |

## Scripts

### `clean.py`

```bash
python scripts/clean.py                    # Clean full corpus (00_raw -> 01_cleaned)
python scripts/clean.py --dry-run          # Report without modifying
python scripts/clean.py --book acts        # Clean specific book
```

### `translate.py`

```bash
python scripts/translate.py --testament new_testament --book john
python scripts/translate.py --testament new_testament --book john --max-files 10
```

### `enrich.py`

```bash
python scripts/enrich.py --testament new_testament --book john
python scripts/enrich.py --testament new_testament --book john --model gpt-4o
```

**Requirements:** `pip install -r requirements.txt` (ftfy, openai, python-dotenv, tqdm)

## Provenance

- **Extraction:** September 2025, hybrid Firecrawl + BeautifulSoup
- **Source:** CatenaBible.com (public domain patristic texts)
- **Integrity:** SHA256 checksums in `data/PROVENANCE.json`
- **Translation model:** GPT-4o-mini
- **Enrichment model:** GPT-4o-mini

## License

Patristic commentaries: **public domain** (AD 100-1700). Dataset, translations, enrichments, and scripts: **CC BY 4.0**.

## Citation

```bibtex
@misc{neuu_bible_commentaries_2026,
  title={Bible Commentaries Dataset: Multi-layered Patristic Commentary Corpus},
  author={NEUU},
  year={2026},
  publisher={GitHub},
  url={https://github.com/neuu-org/bible-commentaries-dataset}
}
```

## Related Datasets (NEUU Ecosystem)

- `bible-crossrefs-dataset` — 1.2M+ cross-references (planned)
- `bible-topics-dataset` — 7,873 unified biblical topics (planned)
- `bible-dictionary-dataset` — Easton's + Smith's Dictionaries (planned)
- `bible-text-dataset` — 20 Bible translations EN + PT-BR (planned)
