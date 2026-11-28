+++
title = "Documentation"
description = "How MeterStore tiers metering time series between PostgreSQL and Apache Iceberg: the storage model, writing and correcting readings, querying across both tiers, reproducible settlement reruns, and operations."
sort_by = "weight"
template = "section.html"
page_template = "page.html"
+++

MeterStore stores [`metering`](https://crates.io/crates/metering) readings in PostgreSQL and Apache Iceberg and queries them through one SQL interface.

[Getting started](@/docs/getting-started.md) walks through deployment.
[Architecture](@/docs/architecture.md) explains the tier boundary.
For settlement reruns or SQL clients outside Rust, see
[Reproducibility](@/docs/reproducibility.md) and [External engines](@/docs/interop.md).
