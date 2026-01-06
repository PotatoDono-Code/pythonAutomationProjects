import orjson
import hashlib
from Ledger.ledger import add_ledger_event

def process_item_file(i_file_path, i_extractor_registry, i_master_table, i_ledger, i_processed_entities, i_batch_id):
    with open(i_file_path, "rb") as file:
        raw_bytes = file.read()
        read_file = orjson.loads(raw_bytes)

        if not isinstance(read_file, dict):
            return False

        if '_id' not in read_file or 'type' not in read_file:
            return False  
          
        entity_hash = hashlib.sha256(raw_bytes).hexdigest()
        id_check = read_file['_id']
        type_check = read_file['type']
        

        add_ledger_event(
            i_ledger = i_ledger, 
            i_batch_id = i_batch_id, 
            i_entity_id = id_check, 
            i_entity_type = type_check, 
            i_entity_hash = entity_hash, 
            i_state = "IN_PROGRESS"
        )

        Extractor = i_extractor_registry[type_check]
        extracted_data = Extractor(read_file).extract_all()

        try:
                
            for sub_table, table_row in extracted_data.items():
                if table_row is None:
                    continue

                target_table = i_master_table[type_check][sub_table]

                if isinstance(table_row, dict):
                    target_table.append(table_row)

                elif isinstance(table_row, list):
                    target_table.extend(table_row)

                else:
                    raise TypeError(f"Unexpected type: {type(table_row)} for table {target_table}")
                
            i_processed_entities.append({
                "entity_id" : id_check,
                "entity_type" : type_check,
                "entity_hash" : entity_hash
            })

            return True

        except Exception:
            add_ledger_event(
                i_ledger = i_ledger, 
                i_batch_id = i_batch_id, 
                i_entity_id = id_check, 
                i_entity_type = type_check, 
                i_entity_hash = entity_hash, 
                i_state = "FAILED"
            )

            return False
                