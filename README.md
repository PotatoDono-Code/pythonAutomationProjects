# 2eSearchAPI
Designed and implemented a full ETL pipeline for 25k+ FoundryVTT PF2e JSON documents. Built a modular extractor architecture (class-based, auto-dispatched by item type) to normalize deeply nested and inconsistent JSON schemas into relational structures.

--- Overview ---
This repository contains a proof of concept ETL pipeline for normalizing Pathfinder Second Edition data from the FoundryVTT system. Foundry stores PF2E content as deeply nested JSON intended for UI consumption rather than analytics or search. This project converts that data into relational, schema stable Parquet datasets suitable for querying and API backed search.

The current implementation uses spells as the reference entity to validate the architecture and data model.

--- Goals --- 
- Normalize heterogeneous and inconsistently structured JSON into relational tables
- Support incremental ingestion without reprocessing unchanged files
- Isolate schema logic by entity type
- Produce Parquet outputs suitable for analytics and FastAPI consumption
- Provide a foundation for additional PF2E entity extractors

--- Architecture ---
Extractor Framework
Each PF2E entity type is parsed by a dedicated extractor class. All extractors inherit from a shared base extractor that provides:
- Safe nested JSON access
- Consistent ID handling
- A standard extract all interface
Extractor selection is handled by a registry based dispatcher keyed on the JSON type field.

--- Schema Normalization ---
Ingested JSON data contains complex nested structures. Spell data structures include:
- Damage blocks
- Interval and level based heightening
- Traits and traditions
- Ritual specific fields

The spell extractor expands these structures into the following relational tables:
- main
- meta
- details
- damage
- heightening
- heighten_interval
- heighten_levels
- heighten_level_damage
- traits
- traditions
- ritual
Each table is written as an independent Parquet file.

--- Incremental Ingestion ---
The pipeline tracks processed file IDs using a persistent checklist. On subsequent runs:
- Previously processed files are skipped
- Only new or changed entries are parsed
- Parquet tables are merged and deduplicated
This reduces runtime by approximately 70 to 85 percent compared to full reprocessing.

--- Error Handling ---
- All file reads are isolated
- Schema access is defensive
- Single file failures do not halt execution
This allows ingestion to complete even with malformed or incomplete source files.

--- Output Structure ---
2e_master_parquet/
  spell/
    main.parquet
    meta.parquet
    details.parquet
    damage.parquet
    heightening.parquet
    heighten_interval.parquet
    heighten_levels.parquet
    heighten_level_damage.parquet
    traits.parquet
    traditions.parquet
    ritual.parquet
  metadata/
    id_checklist.pkl

--- Current Status ---
- Spell extractor complete
- Modular extractor framework complete
- Incremental ingestion implemented
- Parquet merge and deduplication in progress
- Additional entity types planned
This repository represents a working proof of concept rather than a finished production system.

--- Planned Work ---
- Implement additional entity extractors
- Finalize Parquet merge strategy
- Add FastAPI search endpoints
- Integrate AI for complex querying

--- Technologies ---
- Python
- Pandas
- Orjson
- PyArrow
- Parquet

--- Why This Project ---
PF2E data is difficult to analyze due to deeply nested and inconsistent schemas. This pipeline provides a structured foundation for building search tools, analytics, and data driven reference systems on top of the FoundryVTT ecosystem.
