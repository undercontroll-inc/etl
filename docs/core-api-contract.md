# Contrato de dados ETL → core-api

O ETL escreve, a core-api só lê. Este documento é a fonte de verdade das tabelas e views que a
core-api consome. Qualquer mudança aqui exige ajuste coordenado na core-api.

## Ciclo de vida de uma execução

| `etl_run.status` | Significado | Publicável |
|---|---|---|
| `RUNNING` | execução em andamento; já confirmada no banco antes da coleta | não |
| `SUCCESS` | quality gate `PASS` | sim |
| `PARTIAL` | quality gate `WARN` (por exemplo, uma categoria-alvo sem highlights) | sim |
| `FAILED` | exceção ou quality gate `FAIL` | não |

- `etl_run.quality_report` (JSON): `{"verdict": "PASS|WARN|FAIL", "checks": [...], "metrics": {...}}`.
  Cada check traz `name`, `value`, `verdict`, `warn_below` e `fail_below`. Checks atuais:
  `category_coverage`, `curated_products`, `price_coverage`.
- `etl_run.params.replay_of`: presente quando a execução é um replay (`run --skip-api`). Aponta
  para o `run_id` cujo RAW foi reutilizado (sempre a coleta original, nunca outro replay).
- Thresholds por ambiente: `ETL_QUALITY_MIN_CATEGORY_COVERAGE` (0.5),
  `ETL_QUALITY_MIN_CURATED` (1), `ETL_QUALITY_WARN_CURATED` (5),
  `ETL_QUALITY_MIN_PRICE_COVERAGE` (0.5).

## Publicação

`market_published_snapshot` é o sinal `market_snapshot_ready`:

| Coluna | Tipo | Observação |
|---|---|---|
| `snapshot_id` | bigint PK autoincremento | cresce a cada publicação |
| `run_id` | varchar(36) UNIQUE, FK `etl_run` | um run é publicado no máximo uma vez |
| `bucket_key` | varchar(10) | `YYYY-MM` ou `YYYY-MM-DD` |
| `status` | varchar(20) | `SUCCESS` ou `PARTIAL` (status do run na publicação) |
| `quality_report` | JSON | cópia do relatório do run |
| `published_at` | timestamptz | momento da publicação |

`vw_market_published_snapshot` devolve somente a última linha (maior `published_at`, desempate por
`snapshot_id`). Sem linha nessa view a core-api deve responder `AWAITING_DATA`.

Publicação automática: ao fim de `run_pipeline`, um run `SUCCESS` ou `PARTIAL` é publicado em
uma transação curta separada da carga (`ETL_AUTO_PUBLISH=true`, default). Uma falha na
publicação não altera o status do run; basta publicar manualmente com
`python main.py publish --run-id <uuid>`, que também serve para runs carregados com
`ETL_AUTO_PUBLISH=false`. Publicar o mesmo run de novo é idempotente. Runs `FAILED` não podem
ser publicados e o snapshot anterior continua valendo.

## Linhas imutáveis por execução

`market_snapshot`, `market_trend`, `market_score`, `market_price_observation`,
`market_curated_product` e `market_price_movement` têm uma linha por `run_id`. Reexecutar o
mesmo bucket cria novas linhas e nunca altera as do snapshot publicado. As views abaixo
filtram o `run_id` de `vw_market_published_snapshot`.

## Amostra de ofertas

Presentes em `market_price_observation`, `market_curated_product`, `vw_market_product_current`
e `vw_market_rising_products`:

| Coluna | Significado |
|---|---|
| `available_offer_count` | total informado pela API em `paging.total`; nulo quando a API não informa |
| `observed_offer_count` | ofertas efetivamente lidas (paginação limitada por `ETL_OFFERS_MAX_PAGES`) |
| `valid_price_count` | ofertas observadas com preço positivo; é a base da mediana |
| `sample_complete` | `true` somente quando todas as ofertas disponíveis foram observadas |
| `observed_seller_count` | vendedores distintos na amostra observada |

`market_price_observation` também guarda `lowest_price_item_id`/`lowest_price` (menor preço
observado, sem implicar vencedor de buy box), `currency_id` e `domain_id` do momento da coleta.
Foram removidos `offer_count`, `seller_count`, `winner_item_id` e `winner_price`.
Recomendação para a core-api: confiar na mediana somente com `valid_price_count >= 3`.

## Views

| View | Grão | Filtro |
|---|---|---|
| `vw_market_published_snapshot` | 1 linha | última publicação |
| `vw_market_product_current` | produto | run publicado |
| `vw_market_rising_products` | produto | run publicado, crescimento ou `PRIORIDADE_ALTA` |
| `vw_market_category_summary` | `domain_id` + `bucket_key` | run publicado |
| `vw_market_brand_summary` | `brand_slug` + `bucket_key` | run publicado |
| `vw_market_price_movement` | `domain_id` | run publicado |

`vw_market_category_summary` troca `avg_offer_count` por `avg_valid_price_count`.

### `vw_market_price_movement`

Calculada em Python (`ml_etl/pipeline/price_index.py`) e gravada em `market_price_movement`.
Para cada `domain_id` do run, compara com o snapshot publicado mais recente daquele domínio em
bucket anterior. Só entram pares do mesmo produto (`external_type`, `external_id`) e da mesma
moeda, com mediana de preço nos dois períodos.

| Coluna | Significado |
|---|---|
| `domain_id` | tipo de aparelho |
| `bucket_key` | bucket do run publicado |
| `previous_bucket_key` | bucket comparado; nulo sem histórico |
| `consecutive` | `true` quando `previous_bucket_key` é exatamente o bucket anterior |
| `paired_count` | produtos presentes nos dois períodos |
| `current_product_count` | produtos com preço no período atual |
| `previous_product_count` | produtos com preço no período anterior |
| `current_cohort_median` | mediana atual dos produtos pareados |
| `previous_cohort_median` | mediana anterior dos produtos pareados |
| `price_index_delta_pct` | mediana das variações percentuais por produto pareado; nulo sem pares |

Mudança de composição do ranking não move o índice. Exija `paired_count` mínimo e
`consecutive = true` antes de afirmar tendência.

## Ordem de deploy

1. Deploy do ETL (`init-db` aplica os patches de schema e recria as views).
2. `python main.py run` (publica automaticamente) ou `run` + `publish --run-id`.
3. Deploy da core-api.

Bancos existentes recebem os patches registrados em `schema_patch`. Linhas antigas não têm
contagens de amostra (`observed_offer_count` e `valid_price_count` ficam `0`) e não estão
publicadas; as views só mostram dados depois da primeira publicação.
