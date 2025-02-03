import ast
import hashlib
import json
import sys
import os
import random
import shutil

cwd = os.getcwd()
# Keywords to preserve (Flask, Pandas, PostgreSQL related)
PRESERVE_KEYWORDS = {"Flask", "request", "render_template", "session", "redirect", "url_for", "jsonify", "Response",
                     "db", "cursor", "execute", "fetchone", "fetchall", "query", "commit", "rollback",
                     "DataFrame", "read_sql", "to_sql", "merge", "groupby", "apply", "loc", "iloc"}

# Generate different types of obfuscated names
def generate_obfuscated_name(original_name, mapping):
    if original_name in mapping:
        return mapping[original_name]
    
    hash_digest = hashlib.sha256(original_name.encode()).hexdigest()[:8]
    obfuscation_types = [
        f'var_{hash_digest}',    # Current format
        f'_{hash_digest}',       # Underscore prefix
        f'sa_{hash_digest}',    # 'obf_' prefix
        f'x{hash_digest}',       # Random letter prefix
        f'zx_{hash_digest}'      # Double letter with underscore
    ]
    obfuscated_name = random.choice(obfuscation_types)
    mapping[original_name] = obfuscated_name
    return obfuscated_name

# First script: Extract function and variable names and generate mapping
def create_mapping_file(input_file):

    with open(input_file, 'r', encoding='utf-8') as f:
        original_code = f.read()
    
    
    tree = ast.parse(original_code)
    mapping = {}
    builtins = set(dir(__builtins__))
    
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name not in PRESERVE_KEYWORDS:
            generate_obfuscated_name(node.name, mapping)
        elif isinstance(node, ast.Name) and node.id not in PRESERVE_KEYWORDS and node.id not in builtins:
            generate_obfuscated_name(node.id, mapping)
        elif isinstance(node, ast.arg) and node.arg not in PRESERVE_KEYWORDS and node.arg not in builtins:
            generate_obfuscated_name(node.arg, mapping)
    
# Create the mapping file path in the new folder
    mapping_file = f"{os.path.splitext(input_file)[0]}_map.json"
    with open(mapping_file, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, indent=4)
    
    print(f"Mapping file created: {mapping_file}")
    file_list = [ mapping_file]    
    move_files_personal(file_list)
    return 

def move_files_personal(file_list):
    for filesin in file_list:
        source_path = os.path.join(cwd,  filesin)
        destination_dir = os.path.join(cwd, "personal_data")
        destination_path = os.path.join(destination_dir, filesin)
        # Move the file
        shutil.move(source_path, destination_path)


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: python create_mapping.py <input_file>")
    #     sys.exit(1)
    
    # create_mapping_file(sys.argv[1])
    file_to_obfs = "exch_data.py"
    create_mapping_file(file_to_obfs)
