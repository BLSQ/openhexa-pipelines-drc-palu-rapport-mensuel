# snis_palu_data_update

OpenHEXA pipeline that syncs malaria (palu) data from the shared SNIS dataset into the `rdc-palu-rapports-mensuels` workspace.

## What it does

1. Reads the latest version timestamp of the source dataset (`snis-palu-mensuel-extracts`).
2. Compares it against the timestamp of the last successful run (`config/last_update.json`).
3. If newer data is available, downloads three file groups into `pipelines/snis_palu_data_update/data/`:
   - `pyramid/` — health pyramid metadata (`snis_pyramid_metadata.parquet`)
   - `extracts/` — malaria extract files (`palu_extract_*.parquet`)
   - `population/` — population files (`snis_population_*.parquet`)
4. Updates `config/last_update.json` with the new version timestamp.

If the data is already up to date, the pipeline exits without downloading anything.

## Workspace file layout

```
pipelines/snis_palu_data_update/
├── config/
│   └── last_update.json        # timestamp of the last successful sync
└── data/
    ├── pyramid/
    │   └── snis_pyramid_metadata.parquet
    ├── extracts/
    │   └── palu_extract_*.parquet
    └── population/
        └── snis_population_*.parquet
```

## Source dataset

| Field | Value |
|---|---|
| Dataset ID | `snis-palu-mensuel-extracts` |
| Workspace | `rdc-palu-rapports-mensuels` |

