# ETL Mercado Livre — UnderControll

Pipeline Python 3.11+ que coleta Trends/Highlights do ML, filtra pequeno eletrodoméstico, pontua popularidade e grava Postgres para o `core-api`. O Java **nunca** chama o Mercado Livre.

Contrato com a core-api: `docs/core-api-contract.md`. Prompt de insights: `docs/ai-insights-prompt.md`. Este arquivo é só o que o agente precisa em toda sessão.

## Architecture

`main.py` orquestra `ml_etl/pipeline`. `etl_run` é confirmado antes da coleta; cada etapa grava seu RAW em transação própria; ENRICH busca ficha + ofertas paginadas; TRANSFORM/FILTER/SCORE/LOAD rodam em uma transação e alimentam linhas imutáveis por `run_id` (`market_snapshot` com tudo válido, `market_curated_product` só o domínio, `market_price_movement` por coorte). O quality gate define `SUCCESS`/`PARTIAL`/`FAILED`; runs aceitos são publicados em `market_published_snapshot` e as views leem só o run publicado.

## Commands

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # preencher ML_CLIENT_ID / ML_CLIENT_SECRET

python main.py init-db
python main.py run
python main.py run --skip-api --run-id <uuid>   # replay: novo run com params.replay_of
python main.py publish --run-id <uuid>          # publica run SUCCESS/PARTIAL
python main.py create-views
python main.py status
DB_BACKEND=memory python main.py run

.venv/bin/python -m unittest discover -s tests -t .
.venv/bin/coverage run -m unittest discover -s tests -t . && .venv/bin/coverage report   # >= 80%
```

## Layout

- `ml_etl/api/` — HTTP ML (backoff, token `client_credentials`)
- `ml_etl/pipeline/` — extract, raw, enrich, offers, transform, filters, score, quality, price_index, load, publish
- `ml_etl/domain/` — whitelist, atributos, bucket, rejeição
- `ml_etl/db/` — models, migrations (`schema_patch`), views, upsert dialect-aware
- `tests/fake_api.py` — API falsa; sem rede nos testes

## Constraints

- Preço **somente** de `GET /products/:productId/items`. A ficha de `/products/:id` não tem preço (`buy_box_winner` vem `null` com token de aplicação). Com `client_credentials`, `/items/:id`, `/items/:id/prices` e `/sites/MLB/search` dão **403** — não religar.
- `domain_id` (`MLB-MICROWAVES`) é tipo de aparelho. `category_id` (`MLB73057`) é categoria real (ofertas). Nunca gravar um no campo do outro.
- Score (0–100) mede **popularidade**, não preço. Preço/concorrência ficam em `market_price_observation` / curated, fora da fórmula.
- Testes: `DB_BACKEND=memory`, `os.environ` isolado, `use_dotenv=False`. **Nunca** apontar teste para Postgres/SQLite de desenvolvimento.
- FILTER: blacklist de domínio → blacklist de termo (só se o acessório vier **antes** do primeiro termo de aparelho) → whitelist de domínio → termo → categoria.
- Views que o Java lê: `vw_market_published_snapshot`, `vw_market_product_current`, `vw_market_rising_products`, `vw_market_category_summary`, `vw_market_brand_summary`, `vw_market_price_movement`. Nomes de colunas são contrato (`docs/core-api-contract.md`); mudar exige alinhar a core-api. Recriar com `create-views` depois de mudar `ml_etl/db/views.py`.
- Mudança de schema em tabela existente exige um `Patch` novo em `ml_etl/db/migrations.py` (Postgres via `ALTER`, SQLite via rebuild). Banco novo marca os patches sem executá-los.

## Gotchas

- Highlights misturam `ITEM` / `PRODUCT` / `USER_PRODUCT`; cada tipo tem endpoint diferente. Na prática o ranking de `MLB5726` vem como `PRODUCT`.
- Reexecutar o mesmo `bucket_key` cria um novo conjunto de linhas com outro `run_id`; nada muda nas views até o novo run ser publicado. Replay `--skip-api` cria novo run e reproduz preço a partir das páginas de `/products/:id/items` no RAW.
- `available_offer_count` (paging.total) não é amostra: a mediana vem de `valid_price_count` preços observados.
- `market_snapshot` guarda linhas estruturalmente válidas mesmo fora do domínio; só `market_curated_product` é filtrado.
- Confiança `HIGH` exige bucket **anterior** estrito; o bucket corrente não entra na contagem (determinismo). Histórico (ranks, preços, índice) considera só runs publicados.
- Scheduler roda em UTC (`build_trigger`).
