#!/usr/bin/env python3
"""
enrich.py

Adds AI-structured analysis to already-translated commentaries.
Reads from data/02_translated_enriched/ (requires content_pt to exist).
Adds: ai_summary, argumentative_structure, theological_analysis, spiritual_insight.

Usage:
    python scripts/enrich.py --testament new_testament --book john
    python scripts/enrich.py --testament new_testament --book acts --max-files 10
    python scripts/enrich.py --testament new_testament --book john --model gpt-4o
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

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

SYSTEM_PROMPT = (
    "Voce e um teologo patristico especializado em exegese e analise historica dos Pais da Igreja. "
    "Suas respostas devem refletir conhecimento das tradicoes Alexandrina, Antioquena e Latina. "
    "Responda sempre em formato JSON valido, com estrutura fiel e campos descritivos, "
    "sem inferencias especulativas. Se faltar informacao, use null ou []."
)

ENRICHMENT_PROMPT = """Analise os comentarios abaixo e gere enriquecimento estruturado para cada um.

COMENTARIOS (JSON):
```json
{comments}
```

INSTRUCOES:
1. **ai_summary**: one_sentence (max 150 chars), abstract (100-200 palavras), key_points (3-5 itens)
2. **argumentative_structure**: thesis, arguments (2-5), objections (opcional), conclusion
3. **theological_analysis**: doctrines, traditions, church_fathers, theological_method, controversies
4. **spiritual_insight**: theme (1 frase), practical_reflection (1-2 frases)

RESPONDA COM UM JSON ARRAY:
[
  {{
    "comment_id": "author_period_01",
    "ai_summary": {{"one_sentence": "...", "abstract": "...", "key_points": ["..."]}},
    "argumentative_structure": {{"thesis": "...", "arguments": ["..."], "objections": [], "conclusion": "..."}},
    "theological_analysis": {{"doctrines": [], "traditions": [], "church_fathers": [], "theological_method": "", "controversies": []}},
    "spiritual_insight": {{"theme": "...", "practical_reflection": "..."}}
  }}
]"""

PRICES_PER_MILLION = {
    "gpt-4o-mini": (0.150, 0.600),
    "gpt-4o": (2.50, 10.00),
}

REPO_ROOT = Path(__file__).parent.parent
LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_") or "unknown"


def extract_json_array(text: str) -> Optional[list]:
    try:
        if "```json" in text:
            text = text.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in text:
            text = text.split("```", 1)[1].split("```", 1)[0]
        text = text.strip()
        start = text.find("[")
        end = text.rfind("]") + 1
        if start != -1 and end > start:
            return json.loads(text[start:end])
    except Exception:
        pass
    return None


def atomic_write_json(path: Path, data: dict):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


EMPTY_ENRICHMENT = {
    "ai_summary": {"one_sentence": "", "abstract": "", "key_points": []},
    "argumentative_structure": {"thesis": "", "arguments": [], "objections": [], "conclusion": ""},
    "theological_analysis": {"doctrines": [], "traditions": [], "church_fathers": [], "theological_method": "", "controversies": []},
    "spiritual_insight": {"theme": "", "practical_reflection": ""},
}


class Enricher:
    def __init__(self, model: str = "gpt-4o-mini", lang: str = "pt"):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = model
        self.lang = lang
        self._last_call = 0.0
        self.stats = {
            "files": 0, "enriched": 0, "skipped": 0, "no_translation": 0,
            "errors": 0, "cost_usd": 0.0,
            "prompt_tokens": 0, "completion_tokens": 0,
        }

    def _rate_wait(self):
        elapsed = time.time() - self._last_call
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        self._last_call = time.time()

    def enrich_batch(self, payload: List[Dict]) -> tuple:
        minimal = [{"comment_id": p["comment_id"], "author": p["author"],
                     "period": p["period"], "content_pt": p["content_pt"]} for p in payload]
        prompt = ENRICHMENT_PROMPT.replace(
            "{comments}", json.dumps(minimal, ensure_ascii=False))

        for attempt in range(3):
            try:
                self._rate_wait()
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.3,
                    timeout=120.0,
                )

                cost = 0.0
                if resp.usage:
                    pt, ct = resp.usage.prompt_tokens, resp.usage.completion_tokens
                    pi, po = PRICES_PER_MILLION.get(self.model, (0, 0))
                    cost = (pt * pi + ct * po) / 1_000_000
                    self.stats["prompt_tokens"] += pt
                    self.stats["completion_tokens"] += ct
                    self.stats["cost_usd"] += cost

                arr = extract_json_array(resp.choices[0].message.content)
                if isinstance(arr, list) and arr:
                    while len(arr) < len(payload):
                        arr.append({"comment_id": payload[len(arr)]["comment_id"], **EMPTY_ENRICHMENT})
                    return arr[:len(payload)], cost
            except Exception as e:
                err_log = LOG_DIR / "enrich_errors.log"
                with err_log.open("a", encoding="utf-8") as f:
                    f.write(f"{datetime.now().isoformat()} | {e}\n")
                if attempt < 2:
                    time.sleep(2 ** attempt)

        return [{"comment_id": p["comment_id"], **EMPTY_ENRICHMENT} for p in payload], 0.0

    def process_file(self, input_path: Path, output_path: Path, overwrite: bool = False):
        with input_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        comments = data.get("commentaries", [])
        if not comments:
            self.stats["files"] += 1
            return

        # Check if output already exists with enrichment
        if output_path.exists() and not overwrite:
            with output_path.open("r", encoding="utf-8") as f:
                existing = json.load(f)
            existing_comms = existing.get("commentaries", [])
            if existing_comms and all(
                "ai_summary" in c for c in existing_comms if c.get("content_pt")
            ):
                self.stats["files"] += 1
                self.stats["skipped"] += len(comments)
                return

        to_enrich = []
        for idx, c in enumerate(comments):
            has_pt = c.get("content_pt", "").strip() != ""
            has_enrichment = all(k in c for k in ("ai_summary", "argumentative_structure"))
            if has_pt and (not has_enrichment or overwrite):
                to_enrich.append((idx, c))
            elif not has_pt:
                self.stats["no_translation"] += 1

        if not to_enrich:
            self.stats["files"] += 1
            self.stats["skipped"] += len(comments)
            return

        # Build payload
        payload = []
        for idx, c in to_enrich:
            payload.append({
                "comment_id": f"{slugify(c.get('author', 'unknown'))}_{slugify(c.get('period', 'unknown'))}_{idx:02d}",
                "author": c.get("author", ""),
                "period": c.get("period", ""),
                "content_pt": c.get("content_pt", ""),
            })

        enriched, cost = self.enrich_batch(payload)

        for (idx, c), enr in zip(to_enrich, enriched):
            c["ai_summary"] = enr.get("ai_summary", EMPTY_ENRICHMENT["ai_summary"])
            c["argumentative_structure"] = enr.get("argumentative_structure", EMPTY_ENRICHMENT["argumentative_structure"])
            c["theological_analysis"] = enr.get("theological_analysis", EMPTY_ENRICHMENT["theological_analysis"])
            c["spiritual_insight"] = enr.get("spiritual_insight", EMPTY_ENRICHMENT["spiritual_insight"])
            c["enrichment_metadata"] = {
                "model": self.model,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "success",
            }
            self.stats["enriched"] += 1

        data["commentaries"] = comments
        atomic_write_json(output_path, data)
        self.stats["files"] += 1

    def discover_files(self, testament: str, book: str, max_files: int = 0) -> list:
        base = REPO_ROOT / "data" / "02_translated" / self.lang / testament
        files = []
        for p in sorted(base.rglob("verses/*.json")):
            if f"/{book.lower()}/" in str(p).lower().replace("\\", "/"):
                files.append(p)
        if max_files:
            files = files[:max_files]
        return files

    def map_output(self, input_path: Path) -> Path:
        """Map 02_translated/pt/... path to 03_enriched/pt/... path."""
        s = str(input_path)
        # Split after 02_translated/pt/ to get the relative path
        marker = f"02_translated/{self.lang}"
        alt_marker = f"02_translated\\{self.lang}"
        if marker in s:
            rel = s.split(marker, 1)[1]
        elif alt_marker in s:
            rel = s.split(alt_marker, 1)[1]
        else:
            rel = s.split("02_translated", 1)[1]
        return REPO_ROOT / "data" / "03_enriched" / self.lang / rel.lstrip("/\\")

    def run(self, testament: str, book: str, max_files: int = 0, overwrite: bool = False):
        files = self.discover_files(testament, book, max_files)
        print(f"Enriching {len(files)} files ({testament}/{book}) with {self.model}")
        print(f"  Input:  data/02_translated/")
        print(f"  Output: data/03_enriched/")

        if not files:
            print("No translated files found. Run translate.py first.")
            return

        for fp in tqdm(files, desc="Enriching"):
            out = self.map_output(fp)
            try:
                self.process_file(fp, out, overwrite)
            except Exception as e:
                self.stats["errors"] += 1
                err_log = LOG_DIR / "enrich_errors.log"
                with err_log.open("a", encoding="utf-8") as f:
                    f.write(f"{datetime.now().isoformat()} | {fp.name} | {e}\n")

        self._print_summary()

    def _print_summary(self):
        print(f"\n{'='*60}")
        print(f"Enrichment Summary")
        print(f"{'='*60}")
        print(f"Files processed:  {self.stats['files']}")
        print(f"Enriched:         {self.stats['enriched']}")
        print(f"Skipped:          {self.stats['skipped']}")
        print(f"No translation:   {self.stats['no_translation']}")
        print(f"Errors:           {self.stats['errors']}")
        print(f"Cost:             ${self.stats['cost_usd']:.4f}")
        print(f"Tokens:           {self.stats['prompt_tokens']:,} in / {self.stats['completion_tokens']:,} out")
        print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Enrich translated commentaries with AI analysis")
    parser.add_argument("--testament", required=True, help="old_testament or new_testament")
    parser.add_argument("--book", required=True, help="Book name (e.g., john, acts)")
    parser.add_argument("--max-files", type=int, default=0, help="Limit files")
    parser.add_argument("--model", default="gpt-4o-mini", help="OpenAI model")
    parser.add_argument("--lang", default="pt", help="Language code (default: pt)")
    parser.add_argument("--overwrite", action="store_true", help="Re-enrich existing")
    args = parser.parse_args()

    enricher = Enricher(model=args.model, lang=args.lang)
    enricher.run(args.testament, args.book, args.max_files, args.overwrite)


if __name__ == "__main__":
    main()
