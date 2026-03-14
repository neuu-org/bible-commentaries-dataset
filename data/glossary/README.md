# Glossário Teológico Consolidado - Base

Este diretório contém os glossários teológicos usados na tradução de comentários patrísticos.

## Estrutura

- `theological_glossary_base.json` - Termos base (23 termos)
- `learned_terms/` - Termos aprendidos por livro/testamento
- `consolidated_glossary.json` - Glossário completo (base + learned)

## Termos Base (23)

```json
{
  "Trinity": "Trindade",
  "Person": "Pessoa",
  "salvation": "salvação",
  "redemption": "redenção",
  "grace": "graça",
  "faith": "fé",
  "incarnation": "encarnação",
  "hypostatic union": "união hipostática",
  "Holy Spirit": "Espírito Santo",
  "procession": "processão",
  "Church": "Igreja",
  "sacrament": "sacramento",
  "baptism": "batismo",
  "Eucharist": "Eucaristia",
  "resurrection": "ressurreição",
  "judgment": "juízo",
  "eternal life": "vida eterna",
  "sin": "pecado",
  "repentance": "arrependimento",
  "atonement": "expiação",
  "Logos": "Logos",
  "Verbum": "Verbum",
  "Consubstantial": "Consubstancial"
}
```

## Sistema de Aprendizado Dinâmico

O script de tradução analisa os primeiros 50 comentários de cada livro e extrai termos teológicos técnicos automaticamente usando GPT-4o-mini.

**Resultado do teste (João 1:1):**
- Termos base: 23
- Termos aprendidos: 55
- Total: 78 termos

**Custo do aprendizado:**
- ~$0.10 por livro (análise de 50 comentários)
- Glossário reutilizado em todos os demais comentários do livro

## Uso

Os scripts de tradução carregam automaticamente:
1. `theological_glossary_base.json` (sempre)
2. `learned_terms/{testament}_{book}.json` (se existir)
3. Se não existir, executa aprendizado e salva

## Benefícios

✅ **Consistência:** Mesma tradução para termos técnicos em todo o livro
✅ **Qualidade:** Termos aprendidos de contexto real dos Padres da Igreja
✅ **Custo-efetivo:** Aprendizado uma vez, uso ilimitado
✅ **Rastreável:** Cada tradução registra glossário usado (base + learned)
