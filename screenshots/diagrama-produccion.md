# Diagrama de Arquitectura — Producción

## Descripción para draw.io

Usá estos bloques y flechas para armar el diagrama en https://draw.io

---

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            FUENTE DE DATOS                                      │
│                                                                                 │
│   ┌──────────────────┐       ┌──────────────────┐       ┌──────────────────┐   │
│   │   Olist ERP /    │       │   APIs externas   │       │  Otros sistemas  │   │
│   │   E-commerce     │       │   (pagos, envíos) │       │  (CRM, ERP)      │   │
│   └────────┬─────────┘       └────────┬──────────┘       └────────┬─────────┘   │
└────────────┼────────────────────────┼──────────────────────────┼──────────────┘
             │                        │                            │
             └────────────────────────▼────────────────────────────┘
                                      │ CSV / API / SFTP
┌─────────────────────────────────────▼───────────────────────────────────────────┐
│                         INGESTA Y STAGING                                       │
│                                                                                 │
│   ┌──────────────────────────────────────────────────────────────────────────┐  │
│   │                    Google Cloud Storage (GCS)                            │  │
│   │                                                                          │  │
│   │   bucket: olist-raw/                                                     │  │
│   │   ├── landing/YYYY-MM-DD/    ← archivos nuevos sin procesar              │  │
│   │   └── processed/YYYY-MM-DD/ ← archivos ya ingestados (histórico)        │  │
│   └──────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────┬───────────────────────────────────────────┘
                                      │ trigger por evento (Cloud Functions)
                                      │ o schedule (Cloud Composer)
┌─────────────────────────────────────▼───────────────────────────────────────────┐
│                         ORQUESTACIÓN                                            │
│                                                                                 │
│   ┌──────────────────────────────────────────────────────────────────────────┐  │
│   │                  Cloud Composer (Apache Airflow)                         │  │
│   │                                                                          │  │
│   │   DAG: olist_pipeline                                                    │  │
│   │   ├── task: validate_source_files   (chequea que los CSVs existan)       │  │
│   │   ├── task: load_raw               (GCS → BigQuery raw)                  │  │
│   │   ├── task: run_batch              (raw → curated con SQL transforms)    │  │
│   │   ├── task: run_quality_checks     (7+ validaciones → ops)               │  │
│   │   ├── task: alert_on_failures      (si hay ERROR en quality_checks)      │  │
│   │   └── task: archive_source_files   (mueve CSVs a processed/)             │  │
│   │                                                                          │  │
│   │   Schedules:                                                             │  │
│   │   - Batch full:        diario 02:00 UTC                                  │  │
│   │   - Batch incremental: cada 6 horas                                      │  │
│   └──────────────────────────────────────────────────────────────────────────┘  │
└──────────┬──────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           GOOGLE BIGQUERY                                        │
│                                                                                  │
│  ┌─────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐  │
│  │       raw           │   │      curated         │   │        ops           │  │
│  │                     │   │                      │   │                      │  │
│  │ orders              │──▶│ orders               │   │ batch_logs           │  │
│  │ order_items         │──▶│ order_items          │   │ table_metrics        │  │
│  │ order_payments      │──▶│ order_payments       │   │ quality_checks       │  │
│  │ order_reviews       │──▶│ order_reviews        │   │                      │  │
│  │ customers           │──▶│ customers            │   │ ← escrito por        │  │
│  │ products            │──▶│ products             │   │   cada ejecución     │  │
│  │ sellers             │──▶│ sellers              │   │                      │  │
│  │ geolocation         │   │                      │   │                      │  │
│  │ category_trans      │   │ [particionado por    │   │                      │  │
│  │                     │   │  fecha + clustered]  │   │                      │  │
│  │ Sin transformar     │   │ + batch_id           │   │                      │  │
│  │ Retención: 90 días  │   │ + load_date          │   │                      │  │
│  │                     │   │ + source_file        │   │                      │  │
│  └─────────────────────┘   └──────────────────────┘   └──────────────────────┘  │
│                                       │                                          │
│                      Transformación vía BigQuery SQL Jobs                        │
│                      (MERGE para incremental, WRITE_TRUNCATE para full)          │
└───────────────────────────────────────┬──────────────────────────────────────────┘
                                        │
                   ┌────────────────────┼────────────────────┐
                   │                    │                     │
                   ▼                    ▼                     ▼
┌──────────────────────┐  ┌─────────────────────┐  ┌──────────────────────┐
│    Looker Studio     │  │  Cloud Monitoring   │  │  Data Catalog /      │
│                      │  │  + Alerting         │  │  Dataplex            │
│  Dashboards para     │  │                     │  │                      │
│  analistas sobre     │  │  Alertas cuando:    │  │  Linaje de datos     │
│  curated.*           │  │  - quality FAIL     │  │  Gobernanza          │
│                      │  │  - batch FAILED     │  │  Control de accesos  │
│  Acceso: solo lectura│  │  - SLA superado     │  │  por rol             │
└──────────────────────┘  └─────────────────────┘  └──────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────┐
│                      SEGURIDAD Y GOBERNANZA                                      │
│                                                                                  │
│  IAM Roles:                                                                      │
│  ├── data-engineer    → raw (rw), curated (rw), ops (rw)                        │
│  ├── data-analyst     → curated (readonly)                                       │
│  └── pipeline-sa      → service account del Composer, BigQuery Job User         │
│                                                                                  │
│  Cloud Audit Logs → registra quién consulta qué tabla y cuándo                  │
│  Secret Manager   → credenciales y API keys, nunca en código                    │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Componentes clave para draw.io

Cuando lo armes en draw.io usá estos íconos de GCP:

| Componente | Ícono GCP |
|------------|-----------|
| Cloud Storage | Storage > Cloud Storage |
| Cloud Composer | Data Analytics > Cloud Composer |
| BigQuery | Data Analytics > BigQuery |
| Cloud Functions | Serverless > Cloud Functions |
| Cloud Monitoring | Operations > Cloud Monitoring |
| Looker Studio | Data Analytics > Looker |
| IAM | Security > IAM |
| Data Catalog | Data Analytics > Data Catalog |

## Flujo resumido (para la descripción del diagrama)

```
CSV en GCS (landing/)
    → Cloud Composer (DAG scheduler)
        → BigQuery raw (carga directa sin transformar)
        → BigQuery curated (SQL transforms + MERGE incremental)
        → BigQuery ops (métricas + quality checks)
    → Cloud Monitoring (alertas ante fallos)
    → Looker Studio (consumo por analistas)
    → Data Catalog (gobernanza y linaje)
```
