# warehouse/

SQL scripts for the **Amazon Redshift** data warehouse — the serving layer for analytical queries.

## Sub-folders

### `schemas/`
Run once to create the Redshift schemas (namespaces).
```sql
-- 00_create_schemas.sql creates: bronze, silver, gold, reporting
psql -h $REDSHIFT_HOST -U $REDSHIFT_USER -f warehouse/schemas/00_create_schemas.sql
```

### `dimensions/`
DDL scripts for dimension tables (numbered in creation order to respect FK dependencies):
`01_dim_date` → `02_dim_time` → `03_dim_line` → `04_dim_station` → `05_dim_zone` → `06_dim_disruption`

### `facts/`
DDL scripts for fact tables:
- `07_fact_arrivals.sql` — vehicle arrival events
- `08_fact_line_status.sql` — line status snapshots

### `views/`
Pre-built analytical views for BI tools:

| View | Description |
|------|-------------|
| `vw_arrivals_last_hour.sql` | Rolling 1-hour arrival feed |
| `vw_line_performance.sql` | On-time performance by line |
| `vw_station_activity.sql` | Station-level traffic summary |

### `stored_procedures/`
| Procedure | Description |
|-----------|-------------|
| `sp_load_fact_arrivals.sql` | Incremental load into `fact_arrivals` |
| `sp_truncate_staging.sql` | Clean staging tables after load |
| `sp_update_dim_scd2.sql` | SCD Type-2 dimension updates |

### `seeds/`
Python seed scripts to pre-populate static dimensions:
```bash
python warehouse/seeds/seed_dim_date.py   # populates dim_date for 10 years
python warehouse/seeds/seed_dim_time.py   # populates dim_time for all minutes
```

## Deployment Order
1. `schemas/00_create_schemas.sql`
2. `dimensions/` (01 → 06)
3. `facts/` (07 → 08)
4. `views/`
5. `stored_procedures/`
6. Seeds
