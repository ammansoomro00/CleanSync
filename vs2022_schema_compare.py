import os
import subprocess
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get environment variables
SERVER = os.getenv('SERVER')
USERNAME = os.getenv("SQL_USERNAME")
PASSWORD = os.getenv("SQL_PASSWORD")
SOURCE_DB = os.getenv('SOURCE_DB')
TARGET_DB = os.getenv('TARGET_DB')
SQL_PACKAGE_PATH = os.getenv('SQL_PACKAGE_PATH')
FINAL_SCRIPT_NAME = os.getenv('FINAL_SCRIPT_NAME', 'storedProcedures.sql')  
OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'output')  

# Validate required variables
if not all([SERVER, USERNAME, PASSWORD, SOURCE_DB, TARGET_DB]):
    print("❌ Missing required environment variables. Please check your .env file.")
    exit(1)

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Paths
DACPAC_PATH = os.path.join(OUTPUT_DIR, f"{SOURCE_DB}.dacpac")
SCRIPT_PATH = os.path.join(OUTPUT_DIR, "sync_script_main.sql")
FILTERED_SCRIPT_PATH = os.path.join(OUTPUT_DIR, "filtered_sync_script.sql")
CLEANED_SCRIPT_PATH = os.path.join(OUTPUT_DIR, "cleaned_sync_script.sql")  # New intermediate file
FINAL_SCRIPT_PATH = os.path.join(OUTPUT_DIR, FINAL_SCRIPT_NAME)  # Now using the variable from .env

def clean_sql_file(input_path, output_path):
    """Clean the SQL file by removing unnecessary comments and PRINT statements"""
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Remove all PRINT statements
    content = re.sub(r'PRINT N\'.*?\'\s*;\s*GO\s*', '', content)
    
    # Remove -- Alter/Create Procedure/Function comments
    content = re.sub(r'--\s*(Alter|Create)\s+(Procedure|Function):\s*\[?.*?\]?\s*\n', '', content)
    
    # Remove other standalone comments
    content = re.sub(r'--.*?$', '', content, flags=re.MULTILINE)
    
    # Remove block comments
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    
    # Remove multiple empty lines
    content = re.sub(r'\n\s*\n', '\n\n', content)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)

def extract_sql_objects_to_file(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Patterns for CREATE/ALTER and DROP statements
    create_alter_pattern = r'(?i)((CREATE|ALTER)\s+(FUNCTION|PROCEDURE)\s+([^\s(]+).*?AS\s+(?:BEGIN|.*?\nBEGIN).*?END\s*(?:GO|$))'
    drop_pattern = r'(?i)(?:PRINT N\'Dropping (?:Procedure|Function) \[dbo\]\.\[[^\]]+\]\.\.\.\'\s+GO\s+)?(DROP (?:PROCEDURE|FUNCTION) \[dbo\]\.\[([^\]]+)\]\s*;\s*GO)'
    
    # Find all matches
    create_matches = re.finditer(create_alter_pattern, sql_content, re.DOTALL | re.IGNORECASE)
    drop_matches = re.finditer(drop_pattern, sql_content, re.DOTALL | re.IGNORECASE)
    
    # Organize results
    results = {
        'functions': [],
        'procedures': [],
        'drops': []
    }
    
    # Process CREATE/ALTER statements
    for match in create_matches:
        obj_action = match.group(2).lower()
        obj_type = match.group(3).lower()   
        obj_name = match.group(4)
        obj_body = match.group(1).strip()
        
        obj_body = re.sub(r'\n\s*\n', '\n\n', obj_body)
        
        if obj_type == 'function':
            results['functions'].append({
                'name': obj_name,
                'action': obj_action,
                'body': obj_body
            })
        elif obj_type == 'procedure':
            results['procedures'].append({
                'name': obj_name,
                'action': obj_action,
                'body': obj_body
            })
    
    # Process DROP statements
    for match in drop_matches:
        full_statement = match.group(1).strip()
        obj_type = 'procedure' if 'PROCEDURE' in full_statement.upper() else \
                  'function' if 'FUNCTION' in full_statement.upper() else \
                  'table'
        obj_name = match.group(2)
        
        # Reconstruct clean DROP statement without PRINT
        clean_drop = f"DROP {obj_type.upper()} IF EXISTS [dbo].[{obj_name}];"
        
        results['drops'].append({
            'type': obj_type,
            'name': obj_name,
            'body': clean_drop
        })
    
    with open(output_path, 'w', encoding='utf-8') as f:
        # Write DROP statements first (grouped by type)
        if results['drops']:
            f.write("-- DROP STATEMENTS --\n\n")
            for drop in results['drops']:
                f.write(drop['body'])
                f.write("\nGO\n\n")
        
        # Write functions
        if results['functions']:
            f.write("-- FUNCTIONS --\n\n")
            for func in results['functions']:
                f.write(func['body'])
                f.write("\nGO\n\n")
        
        # Write procedures
        if results['procedures']:
            f.write("-- PROCEDURES --\n\n")
            for proc in results['procedures']:
                f.write(proc['body'])
                f.write("\nGO\n\n")
    
    return f"Successfully extracted {len(results['functions'])} functions, {len(results['procedures'])} procedures, and {len(results['drops'])} drop statements to {output_path}"

def convert_sql_file(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Handle PROCEDURE
    def replace_proc(match):
        full_header = match.group(0)
        name = match.group('name')
        modified_header = re.sub(r'\bALTER\b', 'CREATE', full_header, flags=re.IGNORECASE)
        return f"DROP PROCEDURE IF EXISTS {name}\nGO\n{modified_header}"

    content = re.sub(
        r'(?i)(?P<header>(ALTER|CREATE)\s+PROCEDURE\s+(?P<name>(\[[^\]]+\]|\w+)(\.\[[^\]]+\]|\.\w+)?)(\s*\(.*?\))?)',
        replace_proc,
        content,
        flags=re.IGNORECASE | re.DOTALL
    )

    # Handle FUNCTION
    def replace_func(match):
        full_header = match.group(0)
        name = match.group('name')
        modified_header = re.sub(r'\bALTER\b', 'CREATE', full_header, flags=re.IGNORECASE)
        return f"DROP FUNCTION IF EXISTS {name}\nGO\n{modified_header}"

    content = re.sub(
        r'(?i)(?P<header>(ALTER|CREATE)\s+FUNCTION\s+(?P<name>(\[[^\]]+\]|\w+)(\.\[[^\]]+\]|\.\w+)?)(\s*\(.*?\))?)',
        replace_func,
        content,
        flags=re.IGNORECASE | re.DOTALL
    )

    # Clean duplicate GOs and extra spacing
    content = re.sub(r'\bGO\s+GO\b', 'GO', content, flags=re.IGNORECASE)
    content = re.sub(r'\n{3,}', '\n\n', content)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)
        
# Main execution
print("\n===================================================")
print(" STEP 1: Extracting DACPAC from Source Database")
print("===================================================")

extract_cmd = [
    SQL_PACKAGE_PATH,
    "/Action:Extract",
    f"/SourceConnectionString:Data Source={SERVER};Initial Catalog={SOURCE_DB};User ID={USERNAME};Password={PASSWORD};TrustServerCertificate=True",
    f"/TargetFile:{DACPAC_PATH}"
]
def run_command(cmd):
    print("\n➡️ Running command:")
    print(" ".join(cmd))  
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"\n✅ Success:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error:\n{e.stderr}")
        return False

if run_command(extract_cmd):
    print("\n===================================================")
    print(" STEP 2: Generating Schema Sync Script")
    print("===================================================")

    script_cmd = [
        SQL_PACKAGE_PATH,
        "/Action:Script",
        f"/SourceFile:{DACPAC_PATH}",
        f"/TargetConnectionString:Data Source={SERVER};Initial Catalog={TARGET_DB};User ID={USERNAME};Password={PASSWORD};TrustServerCertificate=True",
        f"/OutputPath:{SCRIPT_PATH}",
        "/p:DropObjectsNotInSource=True", 
    ]

    if run_command(script_cmd):
        print("\n===================================================")
        print(" STEP 3: Cleaning SQL Script (Removing Comments)")
        print("===================================================")
        
        clean_sql_file(SCRIPT_PATH, CLEANED_SCRIPT_PATH)
        print(f"Created cleaned intermediate file at: {CLEANED_SCRIPT_PATH}")

        print("\n===================================================")
        print(" STEP 4: Filtering SQL Objects (Functions/Procedures)")
        print("===================================================")
        
        summary_msg = extract_sql_objects_to_file(CLEANED_SCRIPT_PATH, FILTERED_SCRIPT_PATH)
        print(summary_msg)

        print("\n===================================================")
        print(" STEP 5: Converting Script to Use DROP IF EXISTS")
        print("===================================================")

        convert_sql_file(FILTERED_SCRIPT_PATH, FINAL_SCRIPT_PATH)
        print(f"\nFinal cleaned sync script generated at: {FINAL_SCRIPT_PATH}")

        # File size comparison
        original_size = os.path.getsize(SCRIPT_PATH)
        final_size = os.path.getsize(FINAL_SCRIPT_PATH)
        print(f"\nSize reduction: {original_size/1024:.1f}KB → {final_size/1024:.1f}KB ({(original_size-final_size)/1024:.1f}KB saved)")
    else:
        print("\nFailed to generate sync script from DACPAC.")
else:
    print("\nFailed to extract DACPAC from source database.")