# Olist Pipeline — Data Engineer Assessment

Pipeline batch para el dataset **Brazilian E-Commerce Public Dataset by Olist**, construido sobre **Google Cloud BigQuery** con Python y SQL.

---

## Arquitectura general

```
┌─────────────────────────────────────────────────────────────────┐
│                        LOCAL / WSL2                             │
│                                                                 │
│  data/                    scripts/                sql/          │
│  ├── olist_*.csv          ├── setup_bigquery.py   └── curated/  │
│                           ├── load_raw.py             ├── orders.sql
│                           ├── batch_pipeline.py       ├── customers.sql
│                           ├── batch_incremental.py    └── ...   │
│                           └── data_quality.py                   │
└─────────────────────────────┬───────────────────────────────────┘
                              │ BigQuery API
┌─────────────────────────────▼───────────────────────────────────┐
│                  GCP — prueba-elaniin                           │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐ │
│  │     raw      │   │   curated    │   │        ops           │ │
│  │              │   │              │   │                      │ │
│  │ orders       │──▶│ orders       │   │ batch_logs           │ │
│  │ order_items  │──▶│ order_items  │   │ table_metrics        │ │
│  │ order_pay... │──▶│ order_pay... │   │ quality_checks       │ │
│  │ order_rev... │──▶│ order_rev... │   │                      │ │
│  │ customers    │──▶│ customers    │   └──────────────────────┘ │
│  │ products     │──▶│ products     │                            │
│  │ sellers      │──▶│ sellers      │                            │
│  │ geolocation  │   │              │                            │
│  │ category_tr  │   │              │                            │
│  └──────────────┘   └──────────────┘                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Datasets en BigQuery

| Dataset | Propósito |
|---------|-----------|
| `raw` | Datos tal como vienen del CSV, sin transformaciones |
| `curated` | Modelo limpio con transformaciones y trazabilidad |
| `ops` | Logs de ejecución, métricas y controles de calidad |

---

## Estructura del repositorio

```
olist-pipeline/
├── data/                          # CSVs del dataset Olist
├── scripts/
│   ├── setup_bigquery.py          # Parte A: crea datasets y tablas ops
│   ├── load_raw.py                # Parte A: carga CSVs a raw
│   ├── batch_pipeline.py          # Parte C: full batch raw→curated
│   ├── batch_incremental.py       # Parte D: carga delta con MERGE
│   └── data_quality.py            # Parte E: 7 controles de calidad
├── sql/
│   └── curated/                   # SQLs de transformación por tabla
│       ├── orders.sql
│       ├── order_items.sql
│       ├── order_payments.sql
│       ├── order_reviews.sql
│       ├── customers.sql
│       ├── products.sql
│       └── sellers.sql
├── requirements.txt
└── .env                           # Variables de entorno (no commitear)
```

---

## Requisitos

- Python 3.12+
- Google Cloud SDK (`gcloud`)
- Cuenta GCP con BigQuery habilitado (Sandbox)

```bash
pip install -r requirements.txt
```

---

## Configuración

Crear archivo `.env` en la raíz:

```env
GCP_PROJECT=prueba-elaniin
BQ_LOCATION=US
DATA_DIR=data
```

Autenticar con GCP:

```bash
gcloud auth application-default login --no-launch-browser
```

---

## Pasos para ejecutar

### 1. Setup inicial (una sola vez)

Crea los 3 datasets y las tablas operativas en BigQuery:

```bash
python3 scripts/setup_bigquery.py
```

### 2. Cargar datos a raw

Carga los 9 CSVs al dataset `raw` sin transformaciones:

```bash
python3 scripts/load_raw.py
```

Resultado: ~1.55 millones de filas distribuidas en 9 tablas.

### 3. Batch 1 — Carga completa (Parte C)

Transforma y carga todas las tablas de `raw` a `curated`:

```bash
python3 scripts/batch_pipeline.py
```

- Ejecuta los SQLs de `sql/curated/` para cada tabla
- Aplica transformaciones, filtros y deduplicación
- Registra métricas en `ops.batch_logs` y `ops.table_metrics`

### 4. Controles de calidad (Parte E)

Ejecuta 7 validaciones sobre las tablas curated:

```bash
python3 scripts/data_quality.py
```

Resultados guardados en `ops.quality_checks`.

### 5. Batch 2 — Carga incremental (Parte D)

Procesa solo los registros nuevos/actualizados desde el último batch:

```bash
python3 scripts/batch_incremental.py
```

---

## Parte B — Modelo de datos

### Tablas en `curated`

Todas las tablas incluyen los siguientes **campos de trazabilidad**:

| Campo | Descripción |
|-------|-------------|
| `batch_id` | UUID único de la ejecución que cargó el registro |
| `load_date` | Timestamp de cuando se cargó en curated |
| `source_file` | Nombre del archivo CSV de origen |

### Tablas principales

**orders** — Clave: `order_id`
- Timestamps parseados como TIMESTAMP
- `order_status` estandarizado en UPPER

**order_items** — Clave: `(order_id, order_item_id)`
- `total_value` calculado: `price + freight_value`
- Filtro: `price >= 0`

**order_payments** — Clave: `(order_id, payment_sequential)`
- `payment_type` estandarizado en UPPER
- Filtro: `payment_value >= 0`

**order_reviews** — Clave: `review_id`
- Filtro: `review_score BETWEEN 1 AND 5`
- Textos vacíos convertidos a NULL

**customers** — Clave: `customer_id`
- `customer_city` en INITCAP
- `customer_state` en UPPER

**products** — Clave: `product_id`
- Join con `product_category_translation` para nombre en inglés

**sellers** — Clave: `seller_id`
- `seller_city` en INITCAP, `seller_state` en UPPER

---

## Parte D — Estrategia incremental

**Clave incremental:** `order_purchase_timestamp` en la tabla `orders`.

**Watermark:** se obtiene el `MAX(order_purchase_timestamp)` del último batch exitoso registrado en `ops.batch_logs`. Si no hay batch previo, se usa el cutoff por defecto `2018-07-01`.

**Lógica UPSERT:** se usa `MERGE` de BigQuery:
- `WHEN MATCHED` → actualiza `order_status` y fechas de entrega
- `WHEN NOT MATCHED` → inserta la orden nueva

**Tablas relacionadas** (order_items, order_payments, order_reviews): se procesan solo los registros vinculados a órdenes nuevas, usando `INSERT` directo sin pisar existentes.

---

## Parte E — Controles de calidad

| # | Control | Tabla | Severidad | Resultado |
|---|---------|-------|-----------|-----------|
| 1 | Campos obligatorios nulos (order_id, customer_id, order_status) | curated.orders | ERROR | PASS |
| 2 | Estados fuera del catálogo válido | curated.orders | ERROR | PASS |
| 3 | Ítems huérfanos sin orden padre | curated.order_items | ERROR | PASS |
| 4 | Duplicados lógicos por order_id | curated.orders | WARNING | PASS |
| 5 | Fechas inválidas (entrega < compra) | curated.orders | ERROR | PASS |
| 6 | Pagos con valor <= 0 | curated.order_payments | WARNING | WARNING (9 casos) |
| 7 | Reviews sin orden correspondiente | curated.order_reviews | WARNING | PASS |

Los resultados de cada ejecución se consultan en BigQuery:
```sql
SELECT * FROM `prueba-elaniin.ops.quality_checks` ORDER BY checked_at DESC;
```

---

## Parte F — Mejoras y recomendaciones para producción

### Particionado y clustering
Las tablas de `curated` deberían particionarse por fecha (`order_purchase_timestamp` para `orders`) y clusterizarse por las columnas de filtro más frecuentes (`order_status`, `customer_state`). Esto reduce costos de consulta en BigQuery significativamente.

### Alertas automáticas de calidad
Integrar los resultados de `ops.quality_checks` con **Cloud Monitoring** o **Looker Studio**. Configurar alertas cuando `failed_count > 0` en controles de severidad ERROR, notificando vía email o Slack al equipo de datos.

### Manejo de re-procesos y fallos
Implementar lógica de retry con backoff exponencial en los jobs. Ante un fallo, el pipeline debería poder re-ejecutarse desde la tabla fallida sin pisar datos ya correctamente cargados (idempotencia). El `batch_id` y el watermark en `ops.batch_logs` ya sientan la base para esto.

### Versionado de esquemas
Usar **BigQuery Schema Evolution** controlado: ante cambios en los CSVs origen (columnas nuevas, renombradas), mantener un registro de versiones del esquema con fecha de vigencia. Esto evita que un cambio upstream rompa silenciosamente el pipeline.

### Contratos de datos
Definir contratos formales entre el equipo que genera los CSVs y el pipeline: tipos de datos esperados, rangos válidos, campos obligatorios. Herramientas como **Great Expectations** o **Soda Core** pueden validar estos contratos antes de la carga a `raw`.

### Gobernanza y seguridad
- **Acceso por roles**: el dataset `raw` debería ser de solo lectura para consumidores; `curated` de lectura para analistas; `ops` restringido al equipo de ingeniería.
- **Linaje de datos**: usar **BigQuery Data Lineage** (via Dataplex) para trazabilidad end-to-end.
- **Auditoría**: habilitar **Cloud Audit Logs** para registrar quién consulta qué tabla y cuándo.
- **PII**: los campos de ciudad y estado de clientes/vendedores podrían requerir enmascaramiento según regulaciones locales (LGPD en Brasil).

### Orquestación
Reemplazar la ejecución manual de scripts por un orquestador como **Cloud Composer (Airflow)** o **Cloud Workflows**, con DAGs que gestionen dependencias entre tareas, reintentos automáticos y monitoreo centralizado.

---

## Evidencia de ejecución

| Corrida | Tipo | Status | Tablas | Filas |
|---------|------|--------|--------|-------|
| 1 (fallida) | FULL | FAILED | 3 | 135,487 |
| 2 | FULL | SUCCESS | 7 | 549,874 |
| 3 | INCREMENTAL | SUCCESS | 4 | 12,824 |

Las métricas completas están disponibles en `ops.batch_logs` y `ops.table_metrics`.
