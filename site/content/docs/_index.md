+++
title = "Documentation"
description = "How MeterStore tiers metering time series between PostgreSQL and Apache Iceberg: the storage model, writing and correcting readings, querying across both tiers, reproducible settlement reruns, and operations."
sort_by = "weight"
template = "section.html"
page_template = "page.html"
+++

MeterStore is a Rust library. It is not a database, not a service and not an EDM
system: it stores [`metering`](https://crates.io/crates/metering)'s types across
two tiers and serves them through one SQL surface.

Read [Getting started](@/docs/getting-started.md) first, then
[Architecture](@/docs/architecture.md) — almost everything else follows from the
tiering boundary.

If you are evaluating rather than integrating, the two pages that matter are
[Reproducibility](@/docs/reproducibility.md) and
[External engines](@/docs/interop.md): those carry the arguments that are hard to
retrofit later.
