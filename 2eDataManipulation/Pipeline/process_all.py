import glob
import os
import pandas as pd
from pathlib import Path
from Schema.schema_registry import TYPE_REGISTRY
from Extractor.extractor_registry import EXTRACTOR_REGISTRY
from Pipeline.process_item import process_item_file
from Pipeline.writer import write_master_table, write_ledger_file
from datetime import datetime, timezone
from Ledger.ledger import add_ledger_event

def process_all(input_dir):

    extractor_reg = {
        extractor_name: extractor
        for extractor_name, extractor in EXTRACTOR_REGISTRY.items()
    }

    ledger_events = []
    processed_entities = []
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    master_table = {
        type_name: {
            table_name: []
            for table_name in relations
        }
        for type_name, relations in TYPE_REGISTRY.items()
    }

    master_parq_dir = Path("2eDataManipulation/Content")
    master_parq_dir.mkdir(parents = True, exist_ok = True)

    metadata_dir = master_parq_dir/"metadata"
    metadata_dir.mkdir(parents = True, exist_ok = True)
    
    # Create Variable to keep track of how many entries are updated.
    updated = 0

    # Pull all of the file paths to use for the traversing and converting to dataframe information
    # Pull the number of files to use as a counting reference
    json_files = glob.glob(os.path.join(input_dir, "**/*.json"), recursive = True)
    file_count = len(json_files)

    for i, file_path in enumerate(json_files, 1):
        try:
            new_record = process_item_file(
                i_file_path = file_path, 
                i_extractor_registry = extractor_reg,
                i_master_table = master_table,
                i_ledger = ledger_events, 
                i_processed_entities = processed_entities, 
                i_batch_id = batch_id
            )

            if new_record:
                updated += 1

        except Exception as e:
            print(f"{file_path} failed with :: {e}")

        if (i%500) == 0:     
            print(f"Processed Files: {i} of {file_count}")
        
    if updated > 0:
        print(f"Beginning saving {updated} new files")

        try:
            write_master_table(
                i_master_table = master_table, 
                i_master_parq_dir = master_parq_dir,
                i_batch_id = batch_id 
            )
        
        except Exception:
            for entity in processed_entities:
                add_ledger_event(
                    i_ledger = ledger_events, 
                    i_batch_id = batch_id, 
                    i_entity_id = entity["entity_id"], 
                    i_entity_type = entity["entity_type"], 
                    i_entity_hash = entity["entity_hash"], 
                    i_state = "FAILED"
                )
            raise
        
        else: 
            for entity in processed_entities:
                add_ledger_event(
                    i_ledger = ledger_events, 
                    i_batch_id = batch_id, 
                    i_entity_id = entity["entity_id"], 
                    i_entity_type = entity["entity_type"], 
                    i_entity_hash = entity["entity_hash"], 
                    i_state = "COMMITTED"
                )

        write_ledger_file(
            i_ledger = ledger_events, 
            i_metadata_dir = metadata_dir, 
            i_batch_id = batch_id
        )    
                
    else: 
        print("No new entries to process")

        