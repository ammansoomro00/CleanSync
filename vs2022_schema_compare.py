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

# Validate required variables
if not all([SERVER, USERNAME, PASSWORD, SOURCE_DB, TARGET_DB]):
    print("❌ Missing required environment variables. Please check your .env file.")
    exit(1)

# Paths
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DACPAC_PATH = os.path.join(OUTPUT_DIR, f"{SOURCE_DB}.dacpac")
SCRIPT_PATH = os.path.join(OUTPUT_DIR, "sync_script_main.sql")
FILTERED_SCRIPT_PATH = os.path.join(OUTPUT_DIR, "filtered_sync_script.sql")
FINAL_SCRIPT_PATH = os.path.join(OUTPUT_DIR, "final_script.sql")

extract_cmd = [
    SQL_PACKAGE_PATH,
    "/Action:Extract",
    f"/SourceConnectionString:Data Source={SERVER};Initial Catalog={SOURCE_DB};User ID={USERNAME};Password={PASSWORD};TrustServerCertificate=True",
    f"/TargetFile:{DACPAC_PATH}"
]

script_cmd = [
    SQL_PACKAGE_PATH,
    "/Action:Script",
    f"/SourceFile:{DACPAC_PATH}",
    f"/TargetConnectionString:Data Source={SERVER};Initial Catalog={TARGET_DB};User ID={USERNAME};Password={PASSWORD};TrustServerCertificate=True",
    f"/OutputPath:{SCRIPT_PATH}",
    "/p:DropObjectsNotInSource=True", 
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

def extract_sql_objects_to_file(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Remove comments
    sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
    sql_content = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
    
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
        # Write header
        f.write("-- Extracted SQL Objects\n")
        f.write(f"-- From: {input_path}\n")
        f.write(f"-- Total functions: {len(results['functions'])}\n")
        f.write(f"-- Total procedures: {len(results['procedures'])}\n")
        f.write(f"-- Total drop statements: {len(results['drops'])}\n\n")
        
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
                f.write(f"-- {func['action'].title()} Function: {func['name']}\n")
                f.write(func['body'])
                f.write("\nGO\n\n")
        
        # Write procedures
        if results['procedures']:
            f.write("-- PROCEDURES --\n\n")
            for proc in results['procedures']:
                f.write(f"-- {proc['action'].title()} Procedure: {proc['name']}\n")
                f.write(proc['body'])
                f.write("\nGO\n\n")
    
    return f"Successfully extracted {len(results['functions'])} functions, {len(results['procedures'])} procedures, and {len(results['drops'])} drop statements to {output_path}"

def convert_sql_file(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split content into sections that might contain function definitions
    sections = re.split(r'(GO\s*\n)', content)
    processed_sections = []
    
    for i, section in enumerate(sections):
        # Skip GO separators
        if section.strip() == 'GO':
            processed_sections.append(section)
            continue
            
        # Process CREATE/ALTER PROCEDURE statements with PRINT statements
        section = re.sub(
            r'(PRINT\s+N?\'Creating Procedure\s+\[dbo\]\.\[(\w+)\]\...\'\s*;\s*\n)(CREATE|ALTER)\s+PROCEDURE\s+(\w+)',
            lambda m: f'DROP PROCEDURE IF EXISTS {m.group(4)}\nGO\n{m.group(1)}CREATE PROCEDURE {m.group(4)}',
            section,
            flags=re.IGNORECASE
        )

        # Process remaining CREATE/ALTER PROCEDURE statements
        section = re.sub(
            r'(--\s*(Alter|Create)\s+Procedure:\s*\[?([^\n]+)\]?\s*\n)(ALTER|CREATE)\s+PROCEDURE\s+(\w+)',
            lambda m: f'DROP PROCEDURE IF EXISTS {m.group(5)}\nGO\n{m.group(1)}CREATE PROCEDURE {m.group(5)}',
            section,
            flags=re.IGNORECASE
        )

        # Process ALTER PROCEDURE statements without comments
        section = re.sub(
            r'^(ALTER\s+PROCEDURE\s+\[?(\w+)\]?)',
            lambda m: f'DROP PROCEDURE IF EXISTS {m.group(2)}\nGO\nCREATE PROCEDURE {m.group(2)}',
            section,
            flags=re.IGNORECASE
        )

        # Process CREATE/ALTER FUNCTION statements (all types)
        section = re.sub(
            r'(PRINT\s+N?\'Creating Function\s+\[dbo\]\.\[(\w+)\]\...\'\s*;\s*\n)(CREATE|ALTER)\s+FUNCTION\s+(\w+)',
            lambda m: f'DROP FUNCTION IF EXISTS {m.group(4)}\nGO\n{m.group(1)}CREATE FUNCTION {m.group(4)}',
            section,
            flags=re.IGNORECASE
        )

        # Process function declarations with comments
        section = re.sub(
            r'(--\s*(Create|Alter)\s+Function:\s*(\w+)\s*\n)(CREATE|ALTER)\s+FUNCTION\s+(\w+)',
            lambda m: f'DROP FUNCTION IF EXISTS {m.group(5)}\nGO\n{m.group(1)}CREATE FUNCTION {m.group(5)}',
            section,
            flags=re.IGNORECASE
        )

        # Process standalone function declarations at start of section
        if not section.lstrip().startswith('DROP FUNCTION IF EXISTS'):
            section = re.sub(
                r'^(CREATE|ALTER)\s+FUNCTION\s+(\w+)',
                lambda m: f'DROP FUNCTION IF EXISTS {m.group(2)}\nGO\nCREATE FUNCTION {m.group(2)}',
                section,
                flags=re.IGNORECASE
            )

        processed_sections.append(section)

    # Recombine sections
    content = ''.join(processed_sections)
    
    # Remove any duplicate GO statements
    content = re.sub(r'GO\s+GO', 'GO', content)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)

print("\n===================================================")
print(" STEP 1: Extracting DACPAC from Source Database")
print("===================================================")

if run_command(extract_cmd):
    print("\n===================================================")
    print(" STEP 2: Generating Schema Sync Script")
    print("===================================================")

    if run_command(script_cmd):
        print("\n===================================================")
        print(" STEP 3: Filtering SQL Objects (Functions / Procedures)")
        print("===================================================")
        
        summary_msg = extract_sql_objects_to_file(SCRIPT_PATH, FILTERED_SCRIPT_PATH)
        print(summary_msg)

        print("\n===================================================")
        print(" STEP 4: Converting Script to Use DROP IF EXISTS")
        print("===================================================")

        convert_sql_file(FILTERED_SCRIPT_PATH, FINAL_SCRIPT_PATH)

        print(f"\nFinal cleaned sync script generated at: {FINAL_SCRIPT_PATH}")

        # File size comparison
        original_size = os.path.getsize(SCRIPT_PATH)
        filtered_size = os.path.getsize(FILTERED_SCRIPT_PATH)

    else:
        print("\nFailed to generate sync script from DACPAC.")
else:
    print("\nFailed to extract DACPAC from source database.")

    print("\nExtraction failed - cannot generate sync script.")