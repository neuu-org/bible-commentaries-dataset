#!/usr/bin/env python3
"""
translate.py

Translates patristic commentaries from English to Brazilian Portuguese.
Reads from data/01_original/, writes to data/02_translated_enriched/.
Only adds: content_en, content_pt, translation_metadata.

Usage:
    python scripts/translate.py --testament new_testament --book john
    python scripts/translate.py --testament new_testament --book john --max-files 10
    python scripts/translate.py --testament old_testament --book ps --model gpt-4o-mini
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

load_dotenv()

for stream_name in ("stdout", "stderr"):
    stream = getattr(sys, stream_name, None)
    if stream is not None and hasattr(stream, "reconfigure"):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            pass

THEOLOGICAL_GLOSSARY = {
    "Trinity": "Trindade", "Person": "Pessoa", "salvation": "salvacao",
    "grace": "graca", "faith": "fe", "incarnation": "encarnacao",
    "Holy Spirit": "Espirito Santo", "Church": "Igreja",
    "sacrament": "sacramento", "baptism": "batismo",
    "resurrection": "ressurreicao", "sin": "pecado",
    "Logos": "Logos", "Verbum": "Verbum",
}

TRANSLATION_PROMPT = """Traduza os textos teologicos abaixo do ingles para portugues brasileiro academico.

REGRAS:
- Preserve termos latinos/gregos (Logos, Verbum, etc)
- Use glossario: {glossary}
- Mantenha citacoes biblicas no formato original
- Traducao formal e reverente

TEXTOS (JSON):
{texts}

RESPONDA EM JSON:
{{"translations": ["traducao1", "traducao2", ...]}}"""

PRICES_PER_MILLION = {
    "gpt-4o-mini": (0.150, 0.600),
    "gpt-4o": (2.50, 10.00),
}

REPO_ROOT = Path(__file__).parent.parent
# translate reads from CLEANED data, not raw originals
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def extract_json_object(text: str) -> Optional[dict]:
    try:
        if "```json" in text:
            text = text.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in text:
            text = text.split("```", 1)[1].split("```", 1)[0]
        text = text.strip()
        start = text.find("{")
        end = text.rfind("}") + 1
        if start != -1 and end > start:
            return json.loads(text[start:end])
    except Exception:
        pass
    return None


def atomic_write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


class Translator:
    def __init__(self, model: str = "gpt-4o-mini", batch_size: int = 5):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = model
        self.batch_size = batch_size
        self._last_call = 0.0
        self.stats = {
            "files": 0, "translated": 0, "skipped": 0,
            "errors": 0, "cost_usd": 0.0,
            "prompt_tokens": 0, "completion_tokens": 0,
        }

    def _rate_wait(self):
        elapsed = time.time() - self._last_call
        if elapsed < 0.2:
            time.sleep(0.2 - elapsed)
        self._last_call = time.time()

    def translate_batch(self, texts: List[str]) -> tuple:
        glossary = ", ".join(f"{k}->{v}" for k, v in THEOLOGICAL_GLOSSARY.items())
        prompt = TRANSLATION_PROMPT.format(
            glossary=glossary,
            texts=json.dumps(texts, ensure_ascii=False),
        )

        for attempt in range(3):
            try:
                self._rate_wait()
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    timeout=90.0,
                    response_format={"type": "json_object"},
                )

                cost = 0.0
                if resp.usage:
                    pt, ct = resp.usage.prompt_tokens, resp.usage.completion_tokens
                    pi, po = PRICES_PER_MILLION.get(self.model, (0, 0))
                    cost = (pt * pi + ct * po) / 1_000_000
                    self.stats["prompt_tokens"] += pt
                    self.stats["completion_tokens"] += ct
                    self.stats["cost_usd"] += cost

                obj = extract_json_object(resp.choices[0].message.content)
                if obj and isinstance(obj.get("translations"), list):
                    translations = obj["translations"]
                    while len(translations) < len(texts):
                        translations.append("")
                    return translations[:len(texts)], cost
            except Exception as e:
                err_log = LOG_DIR / "translate_errors.log"
                with err_log.open("a", encoding="utf-8") as f:
                    f.write(f"{datetime.now().isoformat()} | {e}\n")
                if attempt < 2:
                    time.sleep(2 ** attempt)

        return [""] * len(texts), 0.0

    def process_file(self, input_path: Path, output_path: Path, overwrite: bool = False):
        with input_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        comments = data.get("commentaries", [])
        if not comments:
            self.stats["files"] += 1
            return

        # Check what needs translation
        to_translate = []
        for idx, c in enumerate(comments):
            has_pt = c.get("content_pt", "").strip() != ""
            src = (c.get("content") or c.get("content_en") or "").strip()
            if src and (not has_pt or overwrite):
                to_translate.append((idx, src))

        if not to_translate:
            self.stats["files"] += 1
            self.stats["skipped"] += len(comments)
            return

        # Load existing output if it exists (to preserve enrichment)
        if output_path.exists():
            with output_path.open("r", encoding="utf-8") as f:
                out_data = json.load(f)
            out_comments = out_data.get("commentaries", comments)
        else:
            out_data = {
                "verse_reference": data.get("verse_reference", ""),
                "source_url": data.get("source_url", ""),
                "commentaries": [dict(c) for c in comments],
            }
            out_comments = out_data["commentaries"]

        # Translate in batches
        for batch_start in range(0, len(to_translate), self.batch_size):
            batch = to_translate[batch_start:batch_start + self.batch_size]
            texts = [src for _, src in batch]
            translations, cost = self.translate_batch(texts)

            for (idx, src), translation in zip(batch, translations):
                if translation:
                    out_comments[idx]["content_en"] = src
                    out_comments[idx]["content_pt"] = translation
                    out_comments[idx]["translation_metadata"] = {
                        "model": self.model,
                        "translated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    self.stats["translated"] += 1

        out_data["commentaries"] = out_comments
        atomic_write_json(output_path, out_data)
        self.stats["files"] += 1

    def discover_files(self, testament: str, book: str, max_files: int = 0) -> list:
        base = REPO_ROOT / "data" / "01_cleaned" / "catena_bible" / testament
        files = []
        for p in sorted(base.rglob("verses/*.json")):
            if f"/{book.lower()}/" in str(p).lower().replace("\\", "/"):
                files.append(p)
        if max_files:
            files = files[:max_files]
        return files

    def map_output(self, input_path: Path) -> Path:
        s = str(input_path)
        rel = s.split("01_cleaned", 1)[1]
        rel = rel.replace("catena_bible/", "").replace("catena_bible\\", "")
        return REPO_ROOT / "data" / "02_translated_enriched" / rel.lstrip("/\\")

    def run(self, testament: str, book: str, max_files: int = 0, overwrite: bool = False):
        files = self.discover_files(testament, book, max_files)
        print(f"Translating {len(files)} files ({testament}/{book}) with {self.model}")

        for fp in tqdm(files, desc="Translating"):
            out = self.map_output(fp)
            try:
                self.process_file(fp, out, overwrite)
            except Exception as e:
                self.stats["errors"] += 1
                err_log = LOG_DIR / "translate_errors.log"
                with err_log.open("a", encoding="utf-8") as f:
                    f.write(f"{datetime.now().isoformat()} | {fp.name} | {e}\n")

        self._print_summary()

    def _print_summary(self):
        print(f"\n{'='*60}")
        print(f"Translation Summary")
        print(f"{'='*60}")
        print(f"Files processed:  {self.stats['files']}")
        print(f"Translated:       {self.stats['translated']}")
        print(f"Skipped:          {self.stats['skipped']}")
        print(f"Errors:           {self.stats['errors']}")
        print(f"Cost:             ${self.stats['cost_usd']:.4f}")
        print(f"Tokens:           {self.stats['prompt_tokens']:,} in / {self.stats['completion_tokens']:,} out")
        print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Translate commentaries EN -> PT-BR")
    parser.add_argument("--testament", required=True, help="old_testament or new_testament")
    parser.add_argument("--book", required=True, help="Book name (e.g., john, acts, ps)")
    parser.add_argument("--max-files", type=int, default=0, help="Limit files to process")
    parser.add_argument("--model", default="gpt-4o-mini", help="OpenAI model")
    parser.add_argument("--batch-size", type=int, default=5, help="Comments per API call")
    parser.add_argument("--overwrite", action="store_true", help="Re-translate existing")
    args = parser.parse_args()

    translator = Translator(model=args.model, batch_size=args.batch_size)
    translator.run(args.testament, args.book, args.max_files, args.overwrite)


if __name__ == "__main__":
    main()
