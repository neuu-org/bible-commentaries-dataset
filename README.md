# Bible Commentaries Dataset

Curated, multi-layered dataset of patristic and historical biblical commentaries spanning **AD 100–1700**, organized for reproducible research in NLP, information retrieval, and theological analysis.

Part of the [NEUU](https://github.com/neuu-org) biblical scholarship ecosystem.

## Overview

This dataset provides verse-level commentaries from the Church Fathers and historical theologians, structured in progressive layers from raw extraction through AI-enriched analysis.

### Current Scale

| Metric | Value |
|--------|-------|
| Original verse files | 31,218 |
| Old Testament coverage | 23,320 verses across 11 categories |
| New Testament coverage | 7,898 verses across 5 categories |
| Translated + AI-enriched | 879 (in progress) |
| Unique authors | 100+ Church Fathers and theologians |
| Time span | ~1,600 years of exegetical tradition |

### Sources

| Source | Type | Status |
|--------|------|--------|
| [CatenaBible.com](https://catenabible.com) | Patristic commentaries | Complete (31,218 files) |
| *Future sources welcome* | — | — |

## Repository Structure

```
bible-commentaries-dataset/
├── data/
│   ├── 00_raw_archive/           # LOCAL ONLY (not tracked by git)
│   │   └── PROVENANCE.json       # Integrity checksums and source metadata
│   │
│   ├── 01_original/              # Canonical normalized verse files
│   │   └── catena_bible/
│   │       ├── old_testament/    # pentateuch, historical, wisdom, prophets...
│   │       └── new_testament/    # gospels, acts, pauline, general, apocalyptic
│   │
│   ├── 02_translated_enriched/   # AI-translated (PT-BR) + structured enrichment
│   │   └── new_testament/
│   │       ├── acts/             # 865 files (85.9% of Acts)
│   │       └── gospels/john/     # 14 files (1.6% of John)
│   │
│   ├── glossary/                 # Theological glossary EN-PT (23 base terms)
│   └── metadata/                 # Author profiles (60+ Church Fathers)
│
└── scripts/
    ├── scrape_catena_bible.py    # How the raw data was extracted
    └── translate_and_enrich.py   # Unified translation + AI enrichment pipeline
```

## Data Layers

### Layer 01 — Original Commentaries

Normalized, per-verse JSON files extracted from source. One file per verse, containing all available commentaries.

**Schema:**
```json
{
  "verse_reference": "ACTS 1:1",
  "verse_text": "...",
  "extraction_date": "2025-09-11",
  "source_url": "https://catenabible.com/acts/1/1",
  "total_commentaries": 12,
  "commentaries": [
    {
      "author": "Bede",
      "period": "AD735",
      "content": "Full commentary text...",
      "content_type": "full",
      "extraction_method": "beautifulsoup"
    }
  ]
}
```

### Layer 02 — Translated + AI-Enriched

Same structure as Layer 01, with additional fields per commentary:

| Field | Description |
|-------|-------------|
| `content_en` | Original English text |
| `content_pt` | Portuguese (BR) translation |
| `translation_metadata` | Model, timestamp |
| `ai_summary` | one_sentence, abstract, key_points |
| `argumentative_structure` | thesis, arguments, objections, conclusion |
| `theological_analysis` | doctrines, traditions, church_fathers, method, controversies |
| `spiritual_insight` | theme, practical_reflection |
| `enrichment_metadata` | Model, timestamp, status |

**Current model:** GPT-4o-mini

### Glossary

23 base theological terms with canonical EN→PT mappings (e.g., Trinity→Trindade, grace→graça). Used during translation to ensure consistency.

### Metadata

Rich author profiles for 60+ Church Fathers including:
- Biographical data (birth/death, location, tradition)
- Theological school and specializations
- Major works and historical impact
- Reliability rating and scholarly consensus

## Biblical Coverage

### Old Testament (23,320 files)

| Category | Books | Files |
|----------|-------|-------|
| Pentateuch | Genesis, Exodus, Leviticus, Numbers, Deuteronomy | 5,851 |
| Historical | Joshua, Judges, Ruth, 1-2 Samuel, 1-2 Kings, 1-2 Chronicles, Ezra, Nehemiah | 6,851 |
| Wisdom Literature | Job, Psalms, Proverbs, Ecclesiastes, Song of Songs, Wisdom, Sirach | 4,785 |
| Major Prophets | Isaiah, Jeremiah, Lamentations, Ezekiel, Daniel, Baruch | 4,544 |
| Minor Prophets | Hosea–Malachi (all 12) | 1,122 |
| Deuterocanonical | Esther, Judith, Tobit, 1-2 Maccabees | 167 |

### New Testament (8,034 files)

| Category | Books | Files |
|----------|-------|-------|
| Gospels | Matthew, Mark, Luke, John | 3,779 |
| Acts | Acts | 1,007 |
| Pauline Epistles | Romans–Philemon (13 letters) | 2,110 |
| General Epistles | Hebrews, James, 1-2 Peter, 1-3 John, Jude | 734 |
| Apocalyptic | Revelation | 404 |

## Scripts

### `scrape_catena_bible.py`

Hybrid web scraper (Firecrawl + BeautifulSoup) that extracts verse-level commentaries from CatenaBible.com. Includes retry logic, deduplication, and progress checkpointing.

### `translate_and_enrich.py`

Unified pipeline that translates commentaries EN→PT-BR and generates structured AI enrichment in a single pass.

```bash
# Translate + enrich a full book
python scripts/translate_and_enrich.py \
    --testament new_testament --book john

# Test with limit
python scripts/translate_and_enrich.py \
    --testament new_testament --book john --max-files 10 --turbo

# Translation only
python scripts/translate_and_enrich.py \
    --testament new_testament --book john --only-translate

# Enrichment only (assumes content_pt exists)
python scripts/translate_and_enrich.py \
    --testament new_testament --book john --only-enrich
```

**Requirements:** `openai`, `python-dotenv`, `tqdm`

## Provenance

- **Extraction date:** September 2025
- **Method:** Hybrid Firecrawl + BeautifulSoup scraping
- **Source:** CatenaBible.com (public domain patristic texts)
- **Integrity:** SHA256 checksums for 100 sampled files stored in `data/00_raw_archive/PROVENANCE.json`
- **Translation model:** GPT-4o-mini (OpenAI)
- **Enrichment model:** GPT-4o-mini (OpenAI)

## License

The patristic commentaries are **public domain** (original texts from AD 100–1700).

The structured dataset, translations, AI enrichments, and scripts in this repository are released under **CC BY 4.0**.

## Citation

If you use this dataset in academic work:

```bibtex
@misc{neuu_bible_commentaries_2026,
  title={Bible Commentaries Dataset: Multi-layered Patristic Commentary Corpus},
  author={NEUU},
  year={2026},
  publisher={GitHub},
  url={https://github.com/neuu-org/bible-commentaries-dataset}
}
```

## Contributing

Contributions are welcome — new commentary sources, improved translations, additional enrichment layers, or bug fixes. Please open an issue first to discuss scope.

## Related Datasets (NEUU Ecosystem)

*Coming soon:*
- `bible-crossrefs-dataset` — 1.2M+ cross-references from OpenBible + TSK
- `bible-topics-dataset` — 7,873 unified biblical topics (Nave + Torrey + Easton + Smith)
- `bible-dictionary-dataset` — Easton's + Smith's Bible Dictionaries
- `bible-text-dataset` — 20 Bible translations (EN + PT-BR)
