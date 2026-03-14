
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
translate_and_enrich_commentaries_final.py

Pipeline unificado para:
  1) Traduzir comentários patrísticos EN → PT
  2) Enriquecer com sumário, estrutura argumentativa, análise teológica e insight espiritual
  3) Salvar TUDO em: 02_translated_enriched_commentaries/ (estrutura idêntica aos originais)

Estrutura de diretórios (esperada):
  Datasets/commentaries/
  ├── 01_original_commentaries/
  │   └── catena_bible/<testament>/<book>/verses/*.json
  └── 02_translated_enriched_commentaries/
      └── <testament>/<book>/verses/*.json

Uso:
  # Processar 10 arquivos de João (Novo Testamento) com modo turbo
  python translate_and_enrich_commentaries_final.py \
      --testament new_testament --book john --max-files 10 --turbo

  # Livro completo (tradução + enriquecimento)
  python translate_and_enrich_commentaries_final.py \
      --testament new_testament --book john

  # Somente tradução (mantendo enriquecimento existente)
  python translate_and_enrich_commentaries_final.py \
      --testament new_testament --book john --only-translate

  # Somente enriquecimento (assume que content_pt já existe)
  python translate_and_enrich_commentaries_final.py \
      --testament new_testament --book john --only-enrich
"""

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

# ============================================================================
# Config & Helpers
# ============================================================================

load_dotenv()

# Tenta garantir saída UTF-8 (evita UnicodeEncodeError em consoles Windows)
for stream_name in ("stdout", "stderr"):
    stream = getattr(sys, stream_name, None)
    if stream is not None and hasattr(stream, "reconfigure"):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            pass

THEOLOGICAL_GLOSSARY = {
    "Trinity": "Trindade",
    "Person": "Pessoa",
    "salvation": "salvação",
    "grace": "graça",
    "faith": "fé",
    "incarnation": "encarnação",
    "Holy Spirit": "Espírito Santo",
    "Church": "Igreja",
    "sacrament": "sacramento",
    "baptism": "batismo",
    "resurrection": "ressurreição",
    "sin": "pecado",
    "Logos": "Logos",
    "Verbum": "Verbum",
}

BATCH_TRANSLATION_PROMPT = """Traduza os textos teológicos abaixo do inglês para português brasileiro acadêmico.

REGRAS:
- Preserve termos latinos/gregos (Logos, Verbum, etc)
- Use glossário: {glossary}
- Mantenha citações bíblicas no formato original
- Tradução formal e reverente

TEXTOS (JSON):
{texts}

RESPONDA EM JSON:
{{"translations": ["tradução1", "tradução2", ...]}}"""

SYSTEM_PROMPT_ENRICHMENT = (
    "Você é um teólogo patrístico especializado em exegese e análise histórica dos Pais da Igreja. "
    "Suas respostas devem refletir conhecimento das tradições Alexandrina, Antioquena e Latina. "
    "Responda sempre em formato JSON válido, com estrutura fiel e campos descritivos, "
    "sem inferências especulativas. Se faltar informação, use null ou []."
)

ENRICHMENT_PROMPT = """Analise os comentários abaixo e gere enriquecimento estruturado para cada um.
Use o CONTEXTO fornecido (versículo bíblico, autor, período) para precisão.

COMENTÁRIOS (JSON):
```json
{comments}
```

INSTRUÇÕES:
1. **ai_summary**:
   - one_sentence: 1 frase (máx 150 chars)
   - abstract: 100-200 palavras
   - key_points: 3-5 itens
2. **argumentative_structure**:
   - thesis, arguments(2-5), objections(opcional), conclusion
3. **theological_analysis**:
   - doctrines (ex: "Trinity", "Incarnation", "Salvation")
   - traditions (ex: "Alexandrian", "Antiochene", "Latin", "Greek")
   - church_fathers (nomes citados)
   - theological_method ("allegorical", "literal", "typological", "moral")
   - controversies (ex: "Arianism", "Nestorianism", "Gnosticism")
4. **spiritual_insight**:
   - theme: 1 frase
   - practical_reflection: 1-2 frases

RESPONDA COM UM JSON ARRAY (lista de objetos, cada um com comment_id e os 4 campos acima).
Não adicione wrapper {"items": ...}, apenas o array direto:
[
  {{
    "comment_id": "author_period_01",
    "ai_summary": {{
      "one_sentence": "...",
      "abstract": "...",
      "key_points": ["...", "...", "..."]
    }},
    "argumentative_structure": {{
      "thesis": "...",
      "arguments": ["...", "..."],
      "objections": [],
      "conclusion": "..."
    }},
    "theological_analysis": {{
      "doctrines": ["Trinity", "Logos"],
      "traditions": ["Latin"],
      "church_fathers": [],
      "theological_method": "literal",
      "controversies": ["Arianism"]
    }},
    "spiritual_insight": {{
      "theme": "A eternidade do Verbo fortalece a fé.",
      "practical_reflection": "Reconhecer Cristo como eterno aprofunda a confiança na sua divindade."
    }}
  }}
]"""

PRICES_PER_MILLION = {
    # input, output  (USD per 1M tokens)
    "gpt-4o-mini": (0.150, 0.600),
    "gpt-4o": (2.50, 10.00),
    "gpt-4-turbo": (10.00, 30.00),
}

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
ERR_LOG = LOG_DIR / "translate_and_enrich_errors.log"
STATS_LOG = LOG_DIR / "translate_and_enrich_stats.json"
PROGRESS_LOG = LOG_DIR / "translate_and_enrich_progress.log"


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_") or "unknown"


def atomic_write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def extract_json_array(text: str) -> Optional[list]:
    """Extrai um JSON array com tolerância a blocos markdown."""
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
    except Exception as e:
        with ERR_LOG.open("a", encoding="utf-8") as log:
            log.write(f"{datetime.now().isoformat()} | JSON array parse error | {e}\n{text[:400]}...\n\n")
    return None


def extract_json_object(text: str) -> Optional[dict]:
    """Extrai um JSON object com tolerância a blocos markdown."""
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
    except Exception as e:
        with ERR_LOG.open("a", encoding="utf-8") as log:
            log.write(f"{datetime.now().isoformat()} | JSON object parse error | {e}\n{text[:400]}...\n\n")
    return None


# ============================================================================
# Core class
# ============================================================================

class TranslateAndEnrichPipeline:
    def __init__(
        self,
        translate_model: str = "gpt-4o-mini",
        enrich_model: str = "gpt-4o-mini",
        batch_size: int = 5,
        max_workers: int = 4,
        rate_limit_translate: int = 5,
        rate_limit_enrich: int = 1,
        max_chars: Optional[int] = None,
        max_reading_time: Optional[int] = None,
        postpone_file: str = "postponed_comments.json",
    ):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.translate_model = translate_model
        self.enrich_model = enrich_model
        self.batch_size = max(1, batch_size)
        self.max_workers = max(1, max_workers)
        
        # Rate limits separados por modelo
        self.min_interval_translate = 1.0 / max(1, rate_limit_translate)
        self.min_interval_enrich = 1.0 / max(1, rate_limit_enrich)
        self._last_call_ts_translate = 0.0
        self._last_call_ts_enrich = 0.0

        # Limites de processamento
        self.max_chars = max_chars
        self.max_reading_time = max_reading_time
        self.postpone_file = Path(postpone_file)
        self.postponed_comments = []

        self.stats = {
            "files_processed": 0,
            "translated_comments": 0,
            "enriched_comments": 0,
            "skipped_translation": 0,
            "skipped_enrichment": 0,
            "postponed_comments": 0,
            "errors": 0,
            "total_prompt_tokens": 0,
            "total_completion_tokens": 0,
            "total_cost_usd": 0.0,
            "translation_time_ms": 0.0,
            "enrichment_time_ms": 0.0,
            "start_time": time.time(),
            "current_file": "",
            "files_log": [],
        }
    
    def _log_progress(self, message: str):
        """Log com timestamp e mensagem de progresso"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] {message}"
        print(log_msg)  # Console output em tempo real
        with PROGRESS_LOG.open("a", encoding="utf-8") as log:
            log.write(log_msg + "\n")

    # ----------------------------------------
    # Rate limit
    # ----------------------------------------
    def _rate_wait_translate(self):
        """Rate limit para tradução (gpt-4o-mini: 5 RPS)"""
        elapsed = time.time() - self._last_call_ts_translate
        if elapsed < self.min_interval_translate:
            time.sleep(self.min_interval_translate - elapsed)
        self._last_call_ts_translate = time.time()
    
    def _rate_wait_enrich(self):
        """Rate limit para enriquecimento (gpt-4o-mini/gpt-4o: 1 RPS)"""
        elapsed = time.time() - self._last_call_ts_enrich
        if elapsed < self.min_interval_enrich:
            time.sleep(self.min_interval_enrich - elapsed)
        self._last_call_ts_enrich = time.time()

    # ----------------------------------------
    # Límites de processamento (postpone)
    # ----------------------------------------
    def _should_postpone(self, comment: dict) -> bool:
        """Verifica se comentário deve ser adiado baseado em limites"""
        if self.max_chars is None and self.max_reading_time is None:
            return False  # Sem limites, processa tudo
        
        content = comment.get("content", "")
        reading_time_str = comment.get("reading_time", "")
        
        # Checar limite de caracteres
        if self.max_chars and len(content) > self.max_chars:
            self._log_progress(f"    ⏭️  ADIADO (chars): {len(content)} > {self.max_chars}")
            return True
        
        # Checar limite de tempo de leitura
        if self.max_reading_time and reading_time_str:
            try:
                # Parse "< 1 min", "1 min", "5 mins", "98 mins"
                time_val = int(reading_time_str.split()[0]) if reading_time_str[0].isdigit() else 1
                if time_val > self.max_reading_time:
                    self._log_progress(f"    ⏭️  ADIADO (tempo): {time_val}m > {self.max_reading_time}m")
                    return True
            except (ValueError, IndexError):
                pass  # Falha no parse, processa normalmente
        
        return False
    
    def _save_postponed_comments(self):
        """Salva comentários adiados em arquivo JSON"""
        if not self.postponed_comments:
            return
        
        self.postpone_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # Carregar existentes se o arquivo existe
            existing = []
            if self.postpone_file.exists():
                with self.postpone_file.open("r", encoding="utf-8") as f:
                    existing = json.load(f)
            
            # Juntar + salvar
            all_postponed = existing + self.postponed_comments
            with self.postpone_file.open("w", encoding="utf-8") as f:
                json.dump(all_postponed, f, ensure_ascii=False, indent=2)
            
            self._log_progress(f"✅ Salvos {len(self.postponed_comments)} comentários adiados em {self.postpone_file}")
        except Exception as e:
            self._log_progress(f"❌ Erro ao salvar comentários adiados: {e}")


    # ----------------------------------------
    # Path mapping
    # ----------------------------------------
    def map_to_output(self, input_path: Path) -> Path:
        """
        Mapeia 01_original_commentaries/... → 02_translated_enriched_commentaries/...
        mantendo <testament>/<book>/verses/arquivo.json
        """
        s = str(input_path)
        if "01_original_commentaries" in s:
            rel = s.split("01_original_commentaries", 1)[1]
            rel = rel.replace("\\catena_bible\\", "\\").replace("/catena_bible/", "/")
            base = Path(__file__).parent.parent.parent / "Datasets" / "commentaries" / "02_translated_enriched_commentaries"
            return base / rel.lstrip("\\/")
        # fallback: mantém caminho
        base = Path(__file__).parent.parent.parent / "Datasets" / "commentaries" / "02_translated_enriched_commentaries"
        try:
            # tenta manter o mesmo final
            return base / input_path.name
        except Exception:
            return base / "output.json"

    # ----------------------------------------
    # Translation
    # ----------------------------------------
    def translate_batch(self, texts: List[str], batch_num: int = 0, total_batches: int = 0) -> tuple:
        """
        Traduz uma lista de textos. Retorna (lista de traduções, cost_usd, tokens_used_tuple).
        
        Args:
            texts: List of texts to translate
            batch_num: Current batch number (1-indexed)
            total_batches: Total number of batches
        
        Returns:
            Tuple of (translations, cost_usd, (prompt_tokens, completion_tokens))
        """
        glossary = ", ".join([f"{k}→{v}" for k, v in THEOLOGICAL_GLOSSARY.items()])
        prompt = BATCH_TRANSLATION_PROMPT.format(
            glossary=glossary,
            texts=json.dumps(texts, ensure_ascii=False, separators=(",", ":")),
        )

        max_retries = 3
        start_time = time.time()
        
        # Log do request
        if batch_num > 0:
            prompt_preview = prompt[:200].replace('\n', ' ')
            texts_preview = f"{len(texts)} textos (média {sum(len(t) for t in texts)//len(texts) if texts else 0} chars)"
            self._log_progress(f"    📤 REQUEST: Model={self.translate_model} | Prompt≈{len(prompt)} chars | Texts={texts_preview}")
        
        for attempt in range(max_retries):
            try:
                self._rate_wait_translate()
                resp = self.client.chat.completions.create(
                    model=self.translate_model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    timeout=90.0,
                    response_format={"type": "json_object"},
                )
                
                # Log da resposta
                if batch_num > 0:
                    response_text = resp.choices[0].message.content[:150].replace('\n', ' ')
                    self._log_progress(f"    📥 RESPONSE: Status=Success | Content≈{len(resp.choices[0].message.content)} chars | Preview: {response_text}...")
                
                elapsed = time.time() - start_time
                
                # stats
                prompt_tokens = 0
                completion_tokens = 0
                cost = 0.0
                if hasattr(resp, "usage") and resp.usage:
                    prompt_tokens = resp.usage.prompt_tokens
                    completion_tokens = resp.usage.completion_tokens
                    pi, po = PRICES_PER_MILLION.get(self.translate_model, (0, 0))
                    cost = (prompt_tokens * pi + completion_tokens * po) / 1_000_000
                    self.stats["total_prompt_tokens"] += prompt_tokens
                    self.stats["total_completion_tokens"] += completion_tokens
                    self.stats["total_cost_usd"] += cost

                obj = extract_json_object(resp.choices[0].message.content)
                if obj and isinstance(obj.get("translations"), list):
                    translations = obj["translations"]
                    # garante mesmo tamanho (preenche vazios se necessário)
                    if len(translations) < len(texts):
                        translations += [""] * (len(texts) - len(translations))
                    
                    if batch_num > 0:
                        batch_info = f"Batch {batch_num}/{total_batches}"
                        self._log_progress(
                            f"  ├─ {batch_info} ({len(texts)} comentários) ✅ [{elapsed:.2f}s] | "
                            f"Custo: ${cost:.4f} | Tokens: {prompt_tokens}/{completion_tokens}"
                        )
                    
                    return translations[: len(texts)], cost, (prompt_tokens, completion_tokens)
            except Exception as e:
                with ERR_LOG.open("a", encoding="utf-8") as log:
                    log.write(f"{datetime.now().isoformat()} | translate_batch error | {e}\n")
                if attempt < max_retries - 1:
                    time.sleep((2 ** attempt) * 1.0)
        
        # fallback vazio
        if batch_num > 0:
            elapsed = time.time() - start_time
            self._log_progress(f"  ├─ Batch {batch_num}/{total_batches} ❌ FALHOU após {elapsed:.2f}s")
        
        return [""] * len(texts), 0.0, (0, 0)

    # ----------------------------------------
    # Enrichment
    # ----------------------------------------
    def enrich_batch(self, payload: List[Dict], batch_num: int = 0, total_batches: int = 0) -> tuple:
        """
        payload: lista de objetos com (comment_id, verse_reference, author, period, content_pt)
        Retorna tuple de (lista de enriquecimentos, cost_usd, (prompt_tokens, completion_tokens))
        NOTA: content_en removido do payload para reduzir tokens (content_pt é suficiente)
        """
        # Remover content_en e verse_reference para reduzir tamanho do payload
        payload_minimal = []
        for item in payload:
            payload_minimal.append({
                "comment_id": item.get("comment_id"),
                "author": item.get("author"),
                "period": item.get("period"),
                "content_pt": item.get("content_pt"),
            })
        prompt = ENRICHMENT_PROMPT.replace(
            "{comments}",
            json.dumps(payload_minimal, ensure_ascii=False, separators=(",", ":"))
        )
        max_retries = 3
        start_time = time.time()
        
        # Log do request
        if batch_num > 0:
            payload_size = sum(len(json.dumps(p)) for p in payload)
            self._log_progress(f"    📤 REQUEST: Model={self.enrich_model} | Prompt≈{len(prompt)} chars | Payload={len(payload)} comentários (≈{payload_size} chars total)")
        
        for attempt in range(max_retries):
            try:
                self._rate_wait_enrich()
                resp = self.client.chat.completions.create(
                    model=self.enrich_model,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT_ENRICHMENT},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.3,
                    timeout=120.0,
                )
                
                # Log da resposta
                if batch_num > 0:
                    response_text = resp.choices[0].message.content[:150].replace('\n', ' ')
                    self._log_progress(f"    📥 RESPONSE: Status=Success | Content≈{len(resp.choices[0].message.content)} chars | Preview: {response_text}...")
                
                elapsed = time.time() - start_time
                
                # stats
                prompt_tokens = 0
                completion_tokens = 0
                cost = 0.0
                if hasattr(resp, "usage") and resp.usage:
                    prompt_tokens = resp.usage.prompt_tokens
                    completion_tokens = resp.usage.completion_tokens
                    pi, po = PRICES_PER_MILLION.get(self.enrich_model, (0, 0))
                    cost = (prompt_tokens * pi + completion_tokens * po) / 1_000_000
                    self.stats["total_prompt_tokens"] += prompt_tokens
                    self.stats["total_completion_tokens"] += completion_tokens
                    self.stats["total_cost_usd"] += cost

                content = resp.choices[0].message.content
                arr = None
                try:
                    # Tenta parsear como array direto (conforme prompt pede)
                    arr = extract_json_array(content)
                except Exception as e:
                    if batch_num > 0:
                        self._log_progress(f"    ⚠️  JSON array parsing error: {str(e)[:100]}")
                    arr = None

                if isinstance(arr, list) and len(arr) > 0:
                    # Validar e completar array
                    if len(arr) < len(payload):
                        # Recebeu menos items que esperado - preenche mantendo comment_id original
                        if batch_num > 0:
                            self._log_progress(f"    ⚠️  Batch {batch_num}/{total_batches}: {len(arr)}/{len(payload)} items, preenchendo vazios...")
                        # Preserva comment_ids dos items faltantes do payload original
                        for i in range(len(arr), len(payload)):
                            original_id = payload[i].get("comment_id", f"unknown_{i}")
                            arr.append({
                                "comment_id": original_id,
                                "ai_summary": {"one_sentence": "", "abstract": "", "key_points": []},
                                "argumentative_structure": {"thesis": "", "arguments": [], "objections": [], "conclusion": ""},
                                "theological_analysis": {"doctrines": [], "traditions": [], "church_fathers": [], "theological_method": "", "controversies": []},
                                "spiritual_insight": {"theme": "", "practical_reflection": ""},
                            })
                    elif len(arr) > len(payload):
                        # Recebeu mais items - trunca
                        if batch_num > 0:
                            self._log_progress(f"    ⚠️  Batch {batch_num}/{total_batches}: {len(arr)}/{len(payload)} items, truncando...")
                        arr = arr[:len(payload)]
                    
                    if batch_num > 0:
                        # Contar campos gerados (só conta os não-vazios)
                        fields_count = {
                            "ai_summary": sum(1 for x in arr if x.get("ai_summary", {}).get("one_sentence")),
                            "argumentative_structure": sum(1 for x in arr if x.get("argumentative_structure", {}).get("thesis")),
                            "theological_analysis": sum(1 for x in arr if x.get("theological_analysis", {}).get("doctrines")),
                            "spiritual_insight": sum(1 for x in arr if x.get("spiritual_insight", {}).get("theme")),
                        }
                        batch_info = f"Batch {batch_num}/{total_batches}"
                        self._log_progress(
                            f"  ├─ {batch_info} ({len(payload)} comentários) ✅ [{elapsed:.2f}s] | "
                            f"Custo: ${cost:.4f} | Tokens: {prompt_tokens}/{completion_tokens} | "
                            f"Campos: summary({fields_count['ai_summary']}/{len(payload)}) args({fields_count['argumentative_structure']}/{len(payload)}) teol({fields_count['theological_analysis']}/{len(payload)}) spirit({fields_count['spiritual_insight']}/{len(payload)})"
                        )
                    
                    return arr, cost, (prompt_tokens, completion_tokens)
            except Exception as e:
                with ERR_LOG.open("a", encoding="utf-8") as log:
                    log.write(f"{datetime.now().isoformat()} | enrich_batch error | {e}\n")
                if attempt < max_retries - 1:
                    time.sleep((2 ** attempt) * 1.0)
        
        # fallback: estrutura vazia
        if batch_num > 0:
            elapsed = time.time() - start_time
            self._log_progress(f"  ├─ Batch {batch_num}/{total_batches} ❌ FALHOU após {elapsed:.2f}s")
        
        out = []
        for item in payload:
            out.append({
                "comment_id": item.get("comment_id"),
                "ai_summary": {"one_sentence": "", "abstract": "", "key_points": []},
                "argumentative_structure": {"thesis": "", "arguments": [], "objections": [], "conclusion": ""},
                "theological_analysis": {
                    "doctrines": [], "traditions": [], "church_fathers": [],
                    "theological_method": "", "controversies": []
                },
                "spiritual_insight": {"theme": "", "practical_reflection": ""},
            })
        return out, 0.0, (0, 0)

    # ----------------------------------------
    # File processing
    # ----------------------------------------
    def process_file(
        self,
        file_path: Path,
        overwrite_translation: bool = False,
        overwrite_enrichment: bool = False,
        only_translate: bool = False,
        only_enrich: bool = False,
    ) -> bool:
        try:
            verse_ref = file_path.stem  # ex: jn_01_01, mt_01_02
            self.stats["current_file"] = verse_ref
            # Log detalhado mostrando o arquivo sendo processado
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"\n[{timestamp}] 📂 PROCESSANDO: {verse_ref}")
            self._log_progress(f"[{timestamp}] 📂 PROCESSANDO: {verse_ref}")
            
            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            verse_ref_full = data.get("verse_reference") or data.get("verse") or f"Unknown ({verse_ref})"
            source_url = data.get("source_url")

            comments = data.get("commentaries", [])
            if not comments:
                print(f"  ⚠️ Nenhum comentário em {verse_ref}")
                self._log_progress(f"  ⚠️ Nenhum comentário em {verse_ref}")
                return True
            
            print(f"  📖 Versículo: {verse_ref_full} | Total: {len(comments)} comentários")
            self._log_progress(f"  📖 Versículo: {verse_ref_full} | Total: {len(comments)} comentários")

            # ✅ CRIAR E INICIALIZAR ARQUIVO DE OUTPUT IMEDIATAMENTE
            out_path = self.map_to_output(file_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_data = {
                "verse_reference": verse_ref_full,
                "source_url": source_url,
                "commentaries": comments,
            }
            try:
                output_display = out_path.relative_to(Path.cwd())
            except ValueError:
                output_display = out_path
            print(f"  📁 Output: {output_display}")
            
            # =========================================================================
            # 1) TRADUÇÃO + ENRIQUECIMENTO INTEGRADOS (comentário por comentário)
            # =========================================================================
            # Preparar lista de comentários a processar, ordenados por tamanho (menores primeiro)
            to_process = []
            for idx, c in enumerate(comments):
                has_pt = "content_pt" in c and isinstance(c["content_pt"], str) and c["content_pt"].strip() != ""
                has_enriched = all(k in c for k in ("ai_summary", "argumentative_structure", "theological_analysis", "spiritual_insight"))
                
                needs_translation = (not has_pt or overwrite_translation) and ((c.get("content") or c.get("content_en") or "").strip())
                needs_enrichment = (not has_enriched or overwrite_enrichment) and has_pt
                
                if needs_translation or needs_enrichment:
                    content_len = len(c.get("content") or c.get("content_pt") or "")
                    to_process.append((idx, content_len, needs_translation, needs_enrichment))
            
            # Ordenar por tamanho (menores primeiro)
            to_process.sort(key=lambda x: x[1])
            
            # ✅ SE NADA PARA PROCESSAR, PULAR ARQUIVO INTEIRO
            if not to_process:
                print(f"  ⏭️  JÁ PROCESSADO: Todos os {len(comments)} comentários já possuem tradução e enriquecimento")
                self._log_progress(f"  ⏭️  JÁ PROCESSADO: {verse_ref} - todos os {len(comments)} comentários OK")
                self.stats["files_processed"] += 1
                return True
            
            if to_process:
                print(f"\n  ⏱️  PROCESSAMENTO INTEGRADO: {len(to_process)} comentários")
                print(f"     (Ordenados por tamanho: menores primeiro ↓)")
                self._log_progress(f"  ⏱️  PROCESSAMENTO: {len(to_process)} comentários")
                
                process_start = time.time()
                total_cost = 0.0
                processed_count = 0
                
                for step, (idx, content_len, needs_translate, needs_enrich) in enumerate(to_process, 1):
                    c = comments[idx]
                    author = c.get("author", "Unknown")
                    period = c.get("period", "Unknown")
                    
                    print(f"\n  📌 Comentário {step}/{len(to_process)}: {author} ({period}) | {content_len} chars")
                    self._log_progress(f"  📌 Comentário {step}/{len(to_process)}: {author}")
                    
                    # ✅ CHECAR SE DEVE ADIAR (POSTPONE)
                    if self._should_postpone(c):
                        self.postponed_comments.append({
                            "file": str(file_path),
                            "author": author,
                            "period": period,
                            "content_length": len(c.get("content", "")),
                            "reading_time": c.get("reading_time", ""),
                            "comment": c,
                        })
                        self.stats["postponed_comments"] += 1
                        continue  # Pula para o próximo comentário
                    
                    # PASSO 1: TRADUÇÃO (se necessário)
                    if needs_translate and not only_enrich:
                        src = c.get("content") or c.get("content_en") or ""
                        if src.strip():
                            translations, batch_cost, tokens = self.translate_batch([src], 1, 1)
                            total_cost += batch_cost
                            
                            if translations and translations[0]:
                                c["content_en"] = src
                                c["content_pt"] = translations[0]
                                c["translation_metadata"] = {
                                    "model": self.translate_model,
                                    "translated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                                }
                                self.stats["translated_comments"] += 1
                                print(f"     ✅ Traduzido ({len(translations[0])} chars)")
                                self._log_progress(f"     ✅ Traduzido")
                                needs_enrich = True  # Agora enriquecer pois tem tradução
                    
                    # PASSO 2: ENRIQUECIMENTO (se necessário)
                    if needs_enrich and not only_translate:
                        content_pt = c.get("content_pt", "")
                        if content_pt and isinstance(content_pt, str) and content_pt.strip():
                            payload = [{
                                "comment_id": f"{slugify(author)}_{slugify(period)}_{idx:02d}",
                                "author": author,
                                "period": period,
                                "content_pt": content_pt,
                            }]
                            enriched, batch_cost, tokens = self.enrich_batch(payload, 1, 1)
                            total_cost += batch_cost
                            
                            if enriched and len(enriched) > 0:
                                obj = enriched[0]
                                c["ai_summary"] = obj.get("ai_summary", {"one_sentence": "", "abstract": "", "key_points": []})
                                c["argumentative_structure"] = obj.get("argumentative_structure", {"thesis": "", "arguments": [], "objections": [], "conclusion": ""})
                                c["theological_analysis"] = obj.get("theological_analysis", {"doctrines": [], "traditions": [], "church_fathers": [], "theological_method": "", "controversies": []})
                                c["spiritual_insight"] = obj.get("spiritual_insight", {"theme": "", "practical_reflection": ""})
                                c["enrichment_metadata"] = {
                                    "model": self.enrich_model,
                                    "timestamp": datetime.now().isoformat(),
                                    "status": "success",
                                }
                                self.stats["enriched_comments"] += 1
                                print(f"     ✅ Enriquecido (4 campos gerados)")
                                self._log_progress(f"     ✅ Enriquecido")
                    
                    # SALVAR IMEDIATAMENTE
                    out_data["commentaries"] = comments
                    atomic_write_json(out_path, out_data)
                    print(f"     💾 Salvo")
                    self._log_progress(f"     💾 Salvo")
                    processed_count += 1
                
                process_time = time.time() - process_start
                self.stats["translation_time_ms"] += process_time * 1000
                
                print(f"\n  ✅ PROCESSAMENTO CONCLUÍDO")
                print(f"     • Comentários processados: {processed_count}")
                print(f"     • Custo total: ${total_cost:.4f}")
                print(f"     • Tempo decorrido: {process_time:.1f}s")
                self._log_progress(f"  ✅ PROCESSAMENTO CONCLUÍDO: {processed_count} comentários | Custo: ${total_cost:.4f} | Tempo: {process_time:.1f}s")
            
            # ✅ SALVAR FINAL
            out_data["commentaries"] = comments
            atomic_write_json(out_path, out_data)
            self.stats["files_processed"] += 1
            self.stats["files_log"].append({
                "file": verse_ref,
                "output_path": str(out_path),
                "timestamp": datetime.now().isoformat(),
                "translated": self.stats["translated_comments"],
                "enriched": self.stats["enriched_comments"],
            })
            # Log detalhado do arquivo salvo
            try:
                relative_path = out_path.relative_to(Path.cwd())
            except ValueError:
                relative_path = out_path
            print(f"\n  ✅ SALVO: {relative_path}")
            self._log_progress(f"  ✅ SALVO: {relative_path}")
            
            # ✅ SALVAR COMENTÁRIOS ADIADOS
            self._save_postponed_comments()
            
            return True

        except Exception as e:
            import traceback
            print(f"  ❌ ERRO: {file_path} | {e}")
            print(f"  Traceback: {traceback.format_exc()}")
            self._log_progress(f"❌ Erro ao processar {file_path}: {e}")
            with ERR_LOG.open("a", encoding="utf-8") as log:
                log.write(f"{datetime.now().isoformat()} | process_file error | {file_path} | {e}\n")
                log.write(traceback.format_exc() + "\n")
            self.stats["errors"] += 1
            return False

    # ----------------------------------------
    # Discovery & Orchestration
    # ----------------------------------------
    def process_book_parallel(
        self,
        testament: str,
        book: str,
        max_files: Optional[int] = None,
        overwrite_translation: bool = False,
        overwrite_enrichment: bool = False,
        only_translate: bool = False,
        only_enrich: bool = False,
    ):
        base = Path(__file__).parent.parent.parent / "Datasets" / "commentaries" / "01_original_commentaries" / "catena_bible"
        testament_dir = base / testament
        if not testament_dir.exists():
            self._log_progress(f"❌ Diretório não encontrado: {testament_dir}")
            return

        # rglob somente verses/*.json e filtra por livro no caminho
        files = []
        for p in testament_dir.rglob("verses/*.json"):
            s = str(p).lower().replace("\\", "/")
            if f"/{book.lower()}/" in s:
                files.append(p)
        files = sorted(files)
        if max_files:
            files = files[:max_files]

        # ✅ CRIAR ESTRUTURA DE DIRETÓRIOS DE OUTPUT ANTECIPADAMENTE
        output_base = Path(__file__).parent.parent.parent / "Datasets" / "commentaries" / "02_translated_enriched_commentaries"
        for file_path in files:
            out_path = self.map_to_output(file_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)

        self._log_progress("\n" + "=" * 90)
        self._log_progress(f"🚀 PIPELINE: {testament}/{book}")
        self._log_progress(f"📄 Arquivos encontrados: {len(files)}")
        self._log_progress(f"🧠 Modelos: Tradução={self.translate_model} | Enriquecimento={self.enrich_model}")
        self._log_progress(f"⚙️  Parâmetros: Batch={self.batch_size} | Threads={self.max_workers}")
        self._log_progress(f"� Rate Limits: Tradução={1/self.min_interval_translate:.1f} RPS | Enriquecimento={1/self.min_interval_enrich:.1f} RPS")
        self._log_progress(f"�📁 Output: 02_translated_enriched_commentaries/{testament}/{book}/verses/")
        self._log_progress("=" * 90)

        with ThreadPoolExecutor(max_workers=self.max_workers) as exe:
            futures = {
                exe.submit(
                    self.process_file,
                    fp,
                    overwrite_translation,
                    overwrite_enrichment,
                    only_translate,
                    only_enrich,
                ): fp for fp in files
            }
            with tqdm(total=len(files), desc="Processando arquivos", unit="arq") as pbar:
                for fut in as_completed(futures):
                    _ = futures[fut]
                    try:
                        fut.result()
                    except Exception as e:
                        with ERR_LOG.open("a", encoding="utf-8") as log:
                            log.write(f"{datetime.now().isoformat()} | future error | {e}\n")
                        self.stats["errors"] += 1
                    pbar.update(1)

        self.print_summary()

    def print_summary(self):
        dur = time.time() - self.stats["start_time"]
        print("\n" + "=" * 80)
        print("📊 RESUMO FINAL DO PIPELINE")
        print("=" * 80)
        print(f"📁 Arquivos processados:     {self.stats['files_processed']}")
        print(f"✅ Traduções geradas:        {self.stats['translated_comments']} (puladas: {self.stats['skipped_translation']})")
        print(f"✨ Enriquecimentos gerados:   {self.stats['enriched_comments']} (pulados: {self.stats['skipped_enrichment']})")
        print(f"❌ Erros:                    {self.stats['errors']}")
        print(f"💰 Custo total (estimado):   ${self.stats['total_cost_usd']:.4f}")
        print(f"🔢 Tokens:")
        print(f"   - Prompt:     {self.stats['total_prompt_tokens']:,}")
        print(f"   - Completion: {self.stats['total_completion_tokens']:,}")
        print(f"⏱️  Tempo por fase:")
        print(f"   - Tradução:   {self.stats['translation_time_ms']/1000:.1f}s")
        print(f"   - Enriquecimento: {self.stats['enrichment_time_ms']/1000:.1f}s")
        print(f"   - Outro:      {dur - self.stats['translation_time_ms']/1000 - self.stats['enrichment_time_ms']/1000:.1f}s")
        print(f"⏱️  Tempo total:              {dur:.1f}s ({dur/60:.1f} min)")
        if self.stats["files_processed"]:
            print(f"⚡ Velocidade:               {self.stats['files_processed']/max(1.0, dur):.2f} arquivos/seg")
        print(f"📂 Saída:                    02_translated_enriched_commentaries/")
        print("=" * 80)
        self._log_progress("\n" + "=" * 90)
        self._log_progress("📊 RESUMO FINAL")
        self._log_progress("=" * 90)
        self._log_progress(f"📁 Arquivos processados:     {self.stats['files_processed']}")
        self._log_progress(f"✅ Traduções geradas:        {self.stats['translated_comments']} (puladas: {self.stats['skipped_translation']})")
        self._log_progress(f"✨ Enriquecimentos gerados:   {self.stats['enriched_comments']} (pulados: {self.stats['skipped_enrichment']})")
        self._log_progress(f"❌ Erros:                    {self.stats['errors']}")
        self._log_progress(f"💰 Custo total (estimado):   ${self.stats['total_cost_usd']:.4f}")
        self._log_progress(f"🔢 Tokens: prompt={self.stats['total_prompt_tokens']}, completion={self.stats['total_completion_tokens']}")
        self._log_progress(f"⏱️  Tempo por fase:")
        self._log_progress(f"   - Tradução:   {self.stats['translation_time_ms']/1000:.1f}s")
        self._log_progress(f"   - Enriquecimento: {self.stats['enrichment_time_ms']/1000:.1f}s")
        self._log_progress(f"   - Outro:      {dur - self.stats['translation_time_ms']/1000 - self.stats['enrichment_time_ms']/1000:.1f}s")
        self._log_progress(f"⏱️  Tempo total: {dur:.1f}s ({dur/60:.1f} min)")
        if self.stats["files_processed"]:
            self._log_progress(f"⚡ Velocidade: {self.stats['files_processed']/max(1.0, dur):.2f} arquivos/seg")
        self._log_progress("=" * 90)
        
        # salva stats em JSON
        try:
            with STATS_LOG.open("w", encoding="utf-8") as f:
                json.dump(self.stats, f, indent=2, ensure_ascii=False)
            self._log_progress(f"📝 Estatísticas salvas em: logs/translate_and_enrich_stats.json")
        except Exception as e:
            self._log_progress(f"⚠️ Erro ao salvar stats: {e}")


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline unificado de Tradução (EN→PT) + Enriquecimento de comentários patrísticos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Saídas:
  - 02_translated_enriched_commentaries/<testament>/<book>/verses/*.json

Campos por comentário:
  - author, period, (opcional: full_content_url, source_url se existirem no raw)
  - content_en (original), content_pt (tradução)
  - ai_summary, argumentative_structure, theological_analysis, spiritual_insight
  - translation_metadata, enrichment_metadata
"""
    )
    parser.add_argument("--testament", required=True, choices=["old_testament", "new_testament"])
    parser.add_argument("--book", required=True, help="ex: john, genesis, matthew")
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--rate-limit-translate", type=int, default=5, help="RPS para tradução (gpt-4o-mini: padrão 5)")
    parser.add_argument("--rate-limit-enrich", type=int, default=1, help="RPS para enriquecimento (padrão 1, seguro para gpt-4o)")

    parser.add_argument("--translate-model", default="gpt-4o-mini", choices=["gpt-4o-mini", "gpt-4o", "gpt-4-turbo"])
    parser.add_argument("--enrich-model", default="gpt-4o-mini", choices=["gpt-4o-mini", "gpt-4o", "gpt-4-turbo"])

    parser.add_argument("--overwrite-translation", action="store_true")
    parser.add_argument("--overwrite-enrichment", action="store_true")
    parser.add_argument("--only-translate", action="store_true")
    parser.add_argument("--only-enrich", action="store_true")

    parser.add_argument("--turbo", action="store_true", help="Modo turbo: batch=8, workers=6, rate-limit-translate=5, rate-limit-enrich=1")
    
    parser.add_argument("--max-chars", type=int, default=None, help="Máx caracteres por comentário (ex: 8000). Maiores são adiados.")
    parser.add_argument("--max-reading-time", type=int, default=None, help="Máx tempo de leitura em minutos (ex: 15). Maiores são adiados.")
    parser.add_argument("--postpone-file", type=str, default="postponed_comments.json", help="Arquivo para guardar comentários adiados")

    args = parser.parse_args()

    if args.turbo:
        args.batch_size = 8
        args.max_workers = 6
        args.rate_limit_translate = 5
        args.rate_limit_enrich = 1

    pipeline = TranslateAndEnrichPipeline(
        translate_model=args.translate_model,
        enrich_model=args.enrich_model,
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        rate_limit_translate=args.rate_limit_translate,
        rate_limit_enrich=args.rate_limit_enrich,
        max_chars=args.max_chars,
        max_reading_time=args.max_reading_time,
        postpone_file=args.postpone_file,
    )

    pipeline.process_book_parallel(
        testament=args.testament,
        book=args.book,
        max_files=args.max_files,
        overwrite_translation=args.overwrite_translation,
        overwrite_enrichment=args.overwrite_enrichment,
        only_translate=args.only_translate,
        only_enrich=args.only_enrich,
    )


if __name__ == "__main__":
    main()
