# Contributing

Guide for adding new commentary sources, expanding coverage, or adding translations.

## Adding Commentaries from a New Source

### 1. Scrape to 00_raw

Create a scraper script in `scripts/` (e.g., `scrape_new_source.py`) that outputs verse-level JSON files following the existing convention:

```
data/00_raw/{source_name}/{testament}/{category}/{book}/verses/{book}_{ch}_{vs}.json
```

Each file must contain at minimum:
```json
{
  "verse_reference": "BOOK CHAPTER:VERSE",
  "commentaries": [
    {
      "author": "Author Name",
      "period": "AD####",
      "content": "Full commentary text in original language"
    }
  ]
}
```

Additional metadata fields (extraction_date, source_url, etc.) are welcome in 00_raw — they will be stripped during cleaning.

### 2. Clean

Run clean.py pointing to the new source:

```bash
python scripts/clean.py --input data/00_raw/new_source --output data/01_cleaned/new_source
```

Verify:
```bash
python scripts/validate_schema.py --data-dir data/01_cleaned/new_source
```

### 3. Translate

```bash
python scripts/translate.py --testament new_testament --book john --lang pt
```

### 4. Enrich (optional)

```bash
python scripts/enrich.py --testament new_testament --book john --lang pt
```

### 5. Update manifests

```bash
python scripts/generate_manifest.py --layer 00_raw
python scripts/generate_manifest.py --layer 01_cleaned
```

### 6. Commit

Follow conventional commits:
```
feat: add {source_name} commentaries for {books}
```

---

## Re-scraping Existing Books

If CatenaBible.com has new commentaries for already-scraped books:

1. **Do NOT overwrite 00_raw** — scrape to a temporary directory first
2. Compare with existing: `python scripts/gap_audit.py --source temp_scrape --target data/00_raw/catena_bible`
3. Copy only NEW files (files that don't exist in 00_raw)
4. Re-run clean.py for affected books
5. Update manifests

```bash
# Example: re-scrape Acts to temp, then merge new verses only
python scripts/scrape_catena_bible.py --book acts --output /tmp/acts_rescrape

python scripts/gap_audit.py \
  --source /tmp/acts_rescrape \
  --target data/00_raw/catena_bible/new_testament/acts/acts/verses

# If gap_audit shows new files, copy them manually to 00_raw
# Then re-run clean
python scripts/clean.py --book acts
```

---

## Adding Commentaries for Missing Books

Some CatenaBible pages may have been missed in the original scraping. To check:

```bash
# See which books exist
python scripts/generate_manifest.py --layer 00_raw
# Check manifest.json -> books section
```

To scrape a missing book:

```bash
python scripts/scrape_catena_bible.py --testament old_testament --book habakkuk
```

Then follow the standard pipeline: clean -> translate -> enrich.

---

## Adding a New Translation Language

The pipeline supports multiple languages via the `--lang` flag.

```bash
# Translate entire Acts to Spanish
python scripts/translate.py --testament new_testament --book acts --lang es

# Enrich in Spanish
python scripts/enrich.py --testament new_testament --book acts --lang es
```

This creates:
```
data/02_translated/es/new_testament/acts/...
data/03_enriched/es/new_testament/acts/...
```

### Language-specific considerations

- Update the theological glossary in `translate.py` for the target language
- Verify output quality on a small sample before running on the full corpus
- Each language gets its own `translation_metadata.json`

---

## Adding a New Enrichment Model

To use a different model (e.g., GPT-4o for higher quality):

```bash
python scripts/enrich.py --testament new_testament --book john --lang pt --model gpt-4o
```

The `enrichment_metadata.model` field in each file tracks which model was used. Multiple models can coexist in the same enriched layer.

---

## Pull Request Checklist

- [ ] New scraper script added to `scripts/` (if new source)
- [ ] Data follows naming convention: `{book}_{ch:02d}_{vs:02d}.json`
- [ ] clean.py runs without errors on new data
- [ ] validate_schema.py passes on new data
- [ ] manifest.json updated for affected layers
- [ ] CHANGELOG.md updated
- [ ] No secrets or API keys in committed files
- [ ] Large data files tracked by Git LFS (JSON in data/ directories)

---

## Cost Estimation

Before running translate or enrich on a large batch:

| Operation | Model | Cost per file | 1,000 files | 31,218 files |
|-----------|-------|---------------|-------------|--------------|
| Translate | GPT-4o-mini | ~$0.001 | ~$1 | ~$31 |
| Translate | GPT-4o | ~$0.02 | ~$20 | ~$625 |
| Enrich | GPT-4o-mini | ~$0.01 | ~$10 | ~$312 |
| Enrich | GPT-4o | ~$0.10 | ~$100 | ~$3,122 |

**Recommendation**: Always use `--max-files 10` first to verify output quality and actual cost before running on full books.
