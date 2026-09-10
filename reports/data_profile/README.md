# NASA FIRMS Wildfire Dataset Discovery & Profiling Report

## Overview
- **Coverage Years:** [2026]
- **Total Rows Profiled:** 100,000
- **Total Raw Size (MB):** 964.61 MB
- **Total Files:** 2

## Summary by Year

| Year | Files | Size (MB) | Rows | Duplicates | Date Range | Satellites | Instruments |
|------|-------|-----------|------|------------|------------|------------|-------------|
| 2026 | 2 | 964.6 | 100,000 | 0 | 2026-01-01 to 2026-05-02 | SNPP | SNPP |

## Key Findings & Schema Drift
1. **Archive vs Near Real-Time (NRT) Schema Drift:**
   - 2020–2025 archive files contain 15 columns including `type` (vegetation fire, volcano, static land, offshore).
   - 2026 contains both archive data and an NRT file (`fire_nrt_SV-C2_800029.csv`). The NRT file contains 14 columns (`type` column is absent).
   - `version` in archive data is `'2'`, whereas in NRT it is `'2.0NRT'`. Canonical schema must preserve `version` as String and `type` as nullable Integer.
2. **Confidence Semantics:**
   - VIIRS collection uses categorical confidence levels: `'n'` (nominal), `'l'` (low), `'h'` (high).
3. **Data Quality Observations:**
   - Coordinates fall strictly within valid global bounds: Latitude ∈ [-90, 90], Longitude ∈ [-180, 180].
   - FRP (Fire Radiative Power in MW) is strictly non-negative.
   - Acquisition dates follow standard ISO `YYYY-MM-DD`.
