# ETL Mercado Livre — UnderControll

Pipeline Python 3.11+ que coleta Trends/Highlights do ML, filtra pequeno eletrodoméstico, pontua popularidade e grava Postgres para o `core-api`. O Java **nunca** chama o Mercado Livre.

Detalhe de contrato e prompt de insights: `docs/ai-insights-prompt.md`. Este arquivo é só o que o agente precisa em toda sessão.

## Architecture

`main.py` orquestra `ml_etl/pipeline`. EXTRACT grava RAW auditável; ENRICH busca ficha + ofertas; TRANSFORM/FILTER/SCORE alimentam `market_snapshot` (tudo válido) e `market_curated_product` (só o domínio). Carga é upsert idempotente por `bucket_key` (default mensal `YYYY-MM`).

## Commands

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # preencher ML_CLIENT_ID / ML_CLIENT_SECRET

python main.py init-db
python main.py run
python main.py run --skip-api --run-id <uuid>   # replay a partir do RAW
python main.py create-views
python main.py status
DB_BACKEND=memory python main.py run

.venv/bin/python -m unittest discover -s tests -t .
```

## Layout

- `ml_etl/api/` — HTTP ML (backoff, token `client_credentials`)
- `ml_etl/pipeline/` — extract, raw, enrich, offers, transform, filters, score, load
- `ml_etl/domain/` — whitelist, atributos, bucket, rejeição
- `ml_etl/db/` — models, views, upsert dialect-aware
- `tests/fake_api.py` — API falsa; sem rede nos testes

## Constraints

- Preço **somente** de `GET /products/:productId/items`. A ficha de `/products/:id` não tem preço (`buy_box_winner` vem `null` com token de aplicação). Com `client_credentials`, `/items/:id`, `/items/:id/prices` e `/sites/MLB/search` dão **403** — não religar.
- `domain_id` (`MLB-MICROWAVES`) é tipo de aparelho. `category_id` (`MLB73057`) é categoria real (ofertas). Nunca gravar um no campo do outro.
- Score (0–100) mede **popularidade**, não preço. Preço/concorrência ficam em `market_price_observation` / curated, fora da fórmula.
- Testes: `DB_BACKEND=memory`, `os.environ` isolado, `use_dotenv=False`. **Nunca** apontar teste para Postgres/SQLite de desenvolvimento.
- FILTER: blacklist de domínio → blacklist de termo (só se o acessório vier **antes** do primeiro termo de aparelho) → whitelist de domínio → termo → categoria.
- Views que o Java lê: `vw_market_product_current`, `vw_market_rising_products`, `vw_market_category_summary`, `vw_market_brand_summary`, `vw_market_price_movement`. Recriar com `create-views` depois de mudar `ml_etl/db/views.py`.

## Gotchas

- Highlights misturam `ITEM` / `PRODUCT` / `USER_PRODUCT`; cada tipo tem endpoint diferente. Na prática o ranking de `MLB5726` vem como `PRODUCT`.
- Reexecutar o mesmo `bucket_key` atualiza as mesmas linhas (`ON CONFLICT DO UPDATE`). Replay `--skip-api` deve reproduzir preço a partir dos payloads de `/products/:id/items` no RAW.
- `market_snapshot` guarda linhas estruturalmente válidas mesmo fora do domínio; só `market_curated_product` é filtrado.
- Confiança `HIGH` exige bucket **anterior** estrito; o bucket corrente não entra na contagem (determinismo).
