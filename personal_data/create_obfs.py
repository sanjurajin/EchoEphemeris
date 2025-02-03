

import ast
import hashlib
import json
import sys
import os
import random
import shutil

pwd = os.getcwd()

# Second script: Apply mapping and create obfuscated file
def obfuscate_code(input_file):
    mapping_file = os.path.join(pwd,  f"{os.path.splitext(input_file)[0]}_map.json")
    obfuscated_file = f"{os.path.splitext(input_file)[0]}_obfs.py"
    
    if not os.path.exists(mapping_file):
        print(f"Error: Mapping file '{mapping_file}' not found. Run the first script first.")
        sys.exit(1)
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    
    with open(input_file, 'r', encoding='utf-8') as f:
        original_code = f.read()
    
    tree = ast.parse(original_code)
    
    class ObfuscateTransformer(ast.NodeTransformer):
        def visit_FunctionDef(self, node):
            if node.name in mapping:
                node.name = mapping[node.name]
            
            # Rename function arguments
            for arg in node.args.args:
                if arg.arg in mapping:
                    arg.arg = mapping[arg.arg]
            
            self.generic_visit(node)
            return node
        
        def visit_Name(self, node):
            if node.id in mapping:
                node.id = mapping[node.id]
            return node
    
    obfuscated_tree = ObfuscateTransformer().visit(tree)
    obfuscated_code = ast.unparse(obfuscated_tree)
    
    with open(obfuscated_file, 'w', encoding='utf-8') as f:
        f.write(obfuscated_code)
    
    print(f"Obfuscated file created: {obfuscated_file}")
    file_list = [input_file]
    move_files_personal(file_list)

def move_files_personal(file_list):
    for filesin in file_list:
        source_path = os.path.join(pwd,  filesin)
        destination_dir = os.path.join(pwd, "personal_data")
        destination_path = os.path.join(destination_dir, filesin)
        # Move the file
        shutil.move(source_path, destination_path)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python obfuscate_code.py <input_file>")
        sys.exit(1)
    
    obfuscate_code(sys.argv[1])

