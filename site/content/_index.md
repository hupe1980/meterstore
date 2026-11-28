+++
title = "MeterStore"
description = "Hot/cold tiered storage for metering time series. PostgreSQL for the recent interval window, Apache Iceberg for the history, one SQL surface across both — with MSCONS correction versioning and reproducible settlement reruns."
template = "index.html"
sort_by = "weight"
+++

## Settlement ingestion

At 15-minute resolution, 100,000 channels produce 9.6 million readings a day.
MeterStore archives complete PostgreSQL partitions to Iceberg and records the new
watermark in the same snapshot as the data.

A settlement writer can reject batches that cross the watermark and require
replays to match quality, span, unit, or a declared price. A correction can
name its expected predecessor so two authors cannot both replace the same version.
[Configure a settlement writer](@/docs/writing.md#settlement-ingestion).

## One statement, both tiers

```rust
let result = store.query(r#"
    SELECT meter_local_day("from") AS day, SUM(value) AS kwh
    FROM readings
    WHERE malo_id = '41373559241'
      AND "from" >= '2025-01-01' AND "from" < '2026-01-01'
    GROUP BY 1 ORDER BY 1
"#).await?;

// Every result carries the boundary it was computed against.
result.watermark();         // where cold ended and hot began
result.tiers_scanned();     // [Cold, Hot]
result.touched_hot_tier();  // whether the answer is only valid for now
```

The planner splits the range at the watermark. Use `meter_local_day` for local
calendar days and `meter_balancing_day` for commodity-specific settlement days.

## Reproduce a settlement

Pin an Iceberg snapshot and version ceiling for an archived settlement. To read
what was known at an instant across both tiers, use a transaction-time cutoff.
[Reproducible reads](@/docs/reproducibility.md).

DuckDB and PyIceberg can read the archived Parquet files through the Iceberg
catalog. The logical readings view resolves corrections by version.
[Query from an external engine](@/docs/interop.md).
