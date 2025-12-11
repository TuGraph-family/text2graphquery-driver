import json
import os

# ================= Configuration =================
# Input: Path to your JSON configuration file
INPUT_JSON_FILE = 'spanner_import_config.json'
# Output: Name of the generated Python script
OUTPUT_PY_FILE = 'import_data_to_spanner.py'
# ===============================================

def generate_script():
    # 1. Read external JSON file
    if not os.path.exists(INPUT_JSON_FILE):
        print(f"Error: File {INPUT_JSON_FILE} not found")
        return

    with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
        files_config = data.get('files', [])

    print(f"Reading configuration file: {INPUT_JSON_FILE}, found {len(files_config)} table configurations...")

    # 2. Prepare code block containers
    table_names_code = []     # Code for table list
    column_names_code = []    # Code for column list
    file_names_code = []      # Code for filename list
    conversion_logic_code = "" # Code for type conversion logic

    # Type map: JSON type -> Python conversion function/type
    type_map = {
        "INT64": "int",
        "FLOAT64": "float",
        "BOOL": "str_to_bool", # Custom helper function
        "STRING": "str"
    }

    # 3. Iterate through JSON config to generate code snippets
    for index, file_info in enumerate(files_config):
        # Use filename as table name (remove .csv extension)
        original_filename = file_info['path']
        table_name = original_filename.replace('.csv', '')
        
        # Handle columns
        cols = list(file_info['columns'].keys())
        cols_tuple_str = "(" + ", ".join([f'"{c}"' for c in cols]) + ")"

        # Append to definition lists
        table_names_code.append(f'    "{table_name}"')
        file_names_code.append(f'    "{original_filename}"')
        column_names_code.append(f'    {cols_tuple_str}')

        # Generate type conversion logic
        conversions = []
        for col_idx, (col_name, col_type) in enumerate(file_info['columns'].items()):
            if col_type in ["INT64", "FLOAT64", "BOOL"]:
                func = type_map.get(col_type)
                # Generates: row[0] = int(row[0])
                conversions.append(f'    row[{col_idx}] = {func}(row[{col_idx}])')
        
        # Assemble conversion block for this table
        conversion_block = f'# Table: {table_name}\n'
        conversion_block += f'# Converting columns based on JSON config\n'
        conversion_block += f'for row in values_list[{index}]:\n'
        
        if conversions:
            conversion_block += '\n'.join(conversions) + "\n"
        else:
            conversion_block += '    pass # No type conversion needed for this table\n'
        
        conversion_block += 'i += 1\n'
        conversion_logic_code += conversion_block + "\n"

    # Convert lists to string representation
    table_list_str = "table_list = [\n" + ",\n".join(table_names_code) + "\n]"
    columns_list_str = "columns_list = [\n" + ",\n".join(column_names_code) + "\n]"
    file_list_str = "file_list = [\n" + ",\n".join(file_names_code) + "\n]"

    # 4. Assemble the final script template
    # Note: Delimiters in the generated script default to ",". 
    # Users should change it to "|" manually if needed.
    final_script_content = f'''# Imports the Google Cloud Client Library.
from google.cloud import spanner
import csv

# ====================================================
# Configuration
# ====================================================
# Your Cloud Spanner instance ID.
instance_id = "YOUR_INSTANCE_ID"
# Your Cloud Spanner database ID.
database_id = "YOUR_DATABASE_ID"
# Batch size for inserts
batch_size = 1000
# Path where CSV files are located (current directory by default)
csv_base_path = "./" 
# ====================================================

# Instantiate a client.
spanner_client = spanner.Client()
instance = spanner_client.instance(instance_id)
database = instance.database(database_id)

# Helper: Safely convert CSV strings to Boolean
def str_to_bool(s):
    if not s: return None
    return s.lower() in ('true', '1', 't', 'y', 'yes')

# Generated from JSON config
{table_list_str}

{columns_list_str}

{file_list_str}

# Container for all data
values_list = [[] for _ in range(len(table_list))]

# 1. Read CSV Files
print("Step 1: Reading CSV files...")
for i in range(len(table_list)):
    file_name = file_list[i]
    full_path = f'{{csv_base_path}}{{file_name}}'
    
    try:
        with open(full_path, newline='', encoding='utf-8') as csvfile:
            # NOTE: Check your CSV delimiter! 
            # Use delimiter="," for standard CSV or delimiter="|" for LDBC/Graph data.
            reader = csv.reader(csvfile, skipinitialspace=True, delimiter=",") 
            
            # Skip header
            next(reader)
            
            for row in reader:
                if row: # simple check for empty lines
                    values_list[i].append(row)
        print(f"Loaded {{len(values_list[i])}} rows from {{file_name}}")
    except FileNotFoundError:
        print(f"Warning: File {{full_path}} not found. Lists will be empty for this table.")

# 2. Transform Data Types
print("Step 2: Converting data types...")
i = 0  # Index tracker for values_list

{conversion_logic_code}

# 3. Insert Data into Spanner
print("Step 3: Inserting data...")

for i in range(len(values_list)):
    table_name = table_list[i]
    rows = values_list[i]
    
    if not rows:
        print(f"Skipping {{table_name}} (no data)")
        continue
        
    print(f"Starting insert for {{table_name}}...")
    for j in range(0, len(rows), batch_size):
        chunk = rows[j:min(j+batch_size, len(rows))]
        try:
            with database.batch() as batch:
                batch.insert(
                    table=table_name,
                    columns=columns_list[i],
                    values=chunk,
                )
            # Optional: Print progress for large files
            # print(f"  Inserted [{{j}} to {{j+len(chunk)}}]")
        except Exception as e:
            print(f"Error inserting into {{table_name}} at batch {{j}}: {{e}}")

print("All operations completed.")
'''

    # 5. Write to output file
    with open(OUTPUT_PY_FILE, 'w', encoding='utf-8') as f:
        f.write(final_script_content)
    
    print(f"Success! Generated script '{OUTPUT_PY_FILE}' based on '{INPUT_JSON_FILE}'")

if __name__ == "__main__":
    generate_script()