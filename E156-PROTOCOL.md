# E156 Protocol — World Bank Data Lakehouse

**Project:** wb-data-lakehouse
**E156 Entry:** (pending workbook addition)
**Date Created:** 2026-04-08
**Date Last Updated:** 2026-04-08

## E156 Body

What indicators from the World Bank's open data platform best characterize global health system capacity, economic development, and governance quality across 217 economies? This pipeline curates 145 indicators across six domains — World Development Indicators, Health Nutrition and Population, Poverty, Governance, Education, and Gender — from the World Bank API v2. An API-driven fetch engine downloads paginated JSON for each indicator, promotes through bronze (provenance-tagged CSV) and silver (dual Parquet: native schema plus cross-lakehouse harmonized iso3c/year/indicator_code/value) tiers. The harmonized schema uses a wb_ prefix on indicator codes enabling seamless joins with companion IHME and WHO lakehouses covering 54 African countries and 80 WHO entity sets. Offline test fixtures validate all 43 pipeline paths without network access, covering column validation, type coercion, and schema harmonization. This lakehouse provides the socioeconomic covariate backbone for causal health forecasting and meta-analytic research. The primary limitation is restriction to curated indicator subsets rather than the full World Bank catalog of over 1,600 indicators.

## Links

- **Repository:** https://github.com/mahmood726-cyber/wb-data-lakehouse
- **Dashboard:** N/A (pipeline project, no HTML dashboard)
- **Design Spec:** `docs/superpowers/specs/2026-04-08-wb-data-lakehouse-design.md`
