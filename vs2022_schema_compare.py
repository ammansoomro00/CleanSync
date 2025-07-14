import os
import re
import subprocess
from dotenv import load_dotenv

# --------------------------
# Configuration
# --------------------------

def load_environment_variables():
    """Load and validate all required environment variables"""
    load_dotenv()
    
    required_vars = {
        'SERVER': os.getenv('SERVER'),
        'SQL_USERNAME': os.getenv("SQL_USERNAME"),
        'SQL_PASSWORD': os.getenv("SQL_PASSWORD"),
        'SOURCE_DB': os.getenv('SOURCE_DB'),
        'TARGET_DB': os.getenv('TARGET_DB'),
        'SQL_PACKAGE_PATH': os.getenv('SQL_PACKAGE_PATH')
    }
    
    missing_vars = [name for name, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return {
        **required_vars,
        'FINAL_SCRIPT_NAME': os.getenv('FINAL_SCRIPT_NAME', 'storedProcedures.sql'),
        'OUTPUT_DIR': os.getenv('OUTPUT_DIR', 'output')
    }

# --------------------------
# File Operations
# --------------------------

def ensure_directory_exists(directory):
    """Create directory if it doesn't exist"""
    os.makedirs(directory, exist_ok=True)

def read_file_content(file_path):
    """Read file content with UTF-8 encoding"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def write_file_content(file_path, content):
    """Write content to file with UTF-8 encoding"""
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

# --------------------------
# SQL Processing Functions
# --------------------------

def clean_sql_content(content):
    """Remove unnecessary comments and PRINT statements from SQL"""
    # Remove PRINT statements
    content = re.sub(r'PRINT N\'.*?\'\s*;\s*GO\s*', '', content)
    # Remove procedure/function comments
    content = re.sub(r'--\s*(Alter|Create)\s+(Procedure|Function):\s*\[?.*?\]?\s*\n', '', content)
    # Remove other comments
    content = re.sub(r'--.*?$', '', content, flags=re.MULTILINE)
    # Remove block comments
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    # Collapse multiple empty lines
    return re.sub(r'\n\s*\n', '\n\n', content)

def extract_sql_objects(content):
    """Extract functions, procedures and drop statements from SQL content"""
    create_alter_pattern = r'(?i)((CREATE|ALTER)\s+(FUNCTION|PROCEDURE)\s+([^\s(]+).*?AS\s+(?:BEGIN|.*?\nBEGIN).*?END\s*(?:GO|$))'
    drop_pattern = r'(?i)(?:PRINT N\'Dropping (?:Procedure|Function) \[dbo\]\.\[[^\]]+\]\.\.\.\'\s+GO\s+)?(DROP (?:PROCEDURE|FUNCTION) \[dbo\]\.\[([^\]]+)\]\s*;\s*GO)'
    
    results = {
        'functions': [],
        'procedures': [],
        'drops': []
    }
    
    # Process CREATE/ALTER statements
    for match in re.finditer(create_alter_pattern, content, re.DOTALL | re.IGNORECASE):
        obj_type = match.group(3).lower()
        obj_data = {
            'name': match.group(4),
            'action': match.group(2).lower(),
            'body': re.sub(r'\n\s*\n', '\n\n', match.group(1).strip())
        }
        
        if obj_type == 'function':
            results['functions'].append(obj_data)
        else:
            results['procedures'].append(obj_data)
    
    # Process DROP statements
    for match in re.finditer(drop_pattern, content, re.DOTALL | re.IGNORECASE):
        obj_type = 'procedure' if 'PROCEDURE' in match.group(1).upper() else 'function'
        results['drops'].append({
            'type': obj_type,
            'name': match.group(2),
            'body': f"DROP {obj_type.upper()} IF EXISTS [dbo].[{match.group(2)}];"
        })
    
    return results

def convert_sql_to_drop_create(content):
    """Convert ALTER statements to CREATE with DROP IF EXISTS"""
    # Handle PROCEDURE
    def replace_proc(match):
        name = match.group('name')
        return f"DROP PROCEDURE IF EXISTS {name}\nGO\n{match.group(0).replace('ALTER', 'CREATE')}"
    
    content = re.sub(
        r'(?i)(?P<header>(ALTER|CREATE)\s+PROCEDURE\s+(?P<name>(\[[^\]]+\]|\w+)(\.\[[^\]]+\]|\.\w+)?)(\s*\(.*?\))?)',
        replace_proc,
        content,
        flags=re.IGNORECASE | re.DOTALL
    )
    
    # Handle FUNCTION
    def replace_func(match):
        name = match.group('name')
        return f"DROP FUNCTION IF EXISTS {name}\nGO\n{match.group(0).replace('ALTER', 'CREATE')}"
    
    content = re.sub(
        r'(?i)(?P<header>(ALTER|CREATE)\s+FUNCTION\s+(?P<name>(\[[^\]]+\]|\w+)(\.\[[^\]]+\]|\.\w+)?)(\s*\(.*?\))?)',
        replace_func,
        content,
        flags=re.IGNORECASE | re.DOTALL
    )
    
    # Clean duplicate GOs and spacing
    content = re.sub(r'\bGO\s+GO\b', 'GO', content, flags=re.IGNORECASE)
    return re.sub(r'\n{3,}', '\n\n', content)

# --------------------------
# Command Execution
# --------------------------

def run_command(command, description):
    """Execute a shell command with error handling"""
    print(f"\n➡️ {description}:")
    print(" ".join(command))
    
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"\n✅ Success:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error:\n{e.stderr}")
        return False

# --------------------------
# Main Workflow
# --------------------------

def generate_sync_script(config):
    """Main workflow to generate the SQL sync script"""
    # Setup paths
    dacpac_path = os.path.join(config['OUTPUT_DIR'], f"{config['SOURCE_DB']}.dacpac")
    script_path = os.path.join(config['OUTPUT_DIR'], "sync_script_main.sql")
    cleaned_path = os.path.join(config['OUTPUT_DIR'], "cleaned_sync_script.sql")
    filtered_path = os.path.join(config['OUTPUT_DIR'], "filtered_sync_script.sql")
    final_path = os.path.join(config['OUTPUT_DIR'], config['FINAL_SCRIPT_NAME'])
    
    # Ensure output directory exists
    ensure_directory_exists(config['OUTPUT_DIR'])
    
    print("\n===================================================")
    print(" STEP 1: Extracting DACPAC from Source Database")
    print("===================================================")
    
    extract_cmd = [
        config['SQL_PACKAGE_PATH'],
        "/Action:Extract",
        f"/SourceConnectionString:Data Source={config['SERVER']};Initial Catalog={config['SOURCE_DB']};User ID={config['SQL_USERNAME']};Password={config['SQL_PASSWORD']};TrustServerCertificate=True",
        f"/TargetFile:{dacpac_path}"
    ]
    
    if not run_command(extract_cmd, "Extracting DACPAC"):
        return False
    
    print("\n===================================================")
    print(" STEP 2: Generating Schema Sync Script")
    print("===================================================")
    
    script_cmd = [
        config['SQL_PACKAGE_PATH'],
        "/Action:Script",
        f"/SourceFile:{dacpac_path}",
        f"/TargetConnectionString:Data Source={config['SERVER']};Initial Catalog={config['TARGET_DB']};User ID={config['SQL_USERNAME']};Password={config['SQL_PASSWORD']};TrustServerCertificate=True",
        f"/OutputPath:{script_path}",
        "/p:DropObjectsNotInSource=True", 
    ]
    
    if not run_command(script_cmd, "Generating sync script"):
        return False
    
    print("\n===================================================")
    print(" STEP 3: Cleaning SQL Script (Removing Comments)")
    print("===================================================")
    
    sql_content = read_file_content(script_path)
    cleaned_content = clean_sql_content(sql_content)
    write_file_content(cleaned_path, cleaned_content)
    print(f"Created cleaned intermediate file at: {cleaned_path}")
    
    print("\n===================================================")
    print(" STEP 4: Filtering SQL Objects (Functions/Procedures)")
    print("===================================================")
    
    extracted = extract_sql_objects(cleaned_content)
    filtered_content = generate_filtered_output(extracted)
    write_file_content(filtered_path, filtered_content)
    print(f"Extracted {len(extracted['functions'])} functions, {len(extracted['procedures'])} procedures, and {len(extracted['drops'])} drop statements")
    
    print("\n===================================================")
    print(" STEP 5: Converting Script to Use DROP IF EXISTS")
    print("===================================================")
    
    final_content = convert_sql_to_drop_create(filtered_content)
    write_file_content(final_path, final_content)
    print(f"\nFinal cleaned sync script generated at: {final_path}")
    
    # File size comparison
    original_size = os.path.getsize(script_path)
    final_size = os.path.getsize(final_path)
    print(f"\nSize reduction: {original_size/1024:.1f}KB → {final_size/1024:.1f}KB ({(original_size-final_size)/1024:.1f}KB saved)")
    
    return True

def generate_filtered_output(extracted_objects):
    """Generate the filtered SQL output from extracted objects"""
    output = []
    
    if extracted_objects['drops']:
        output.append("-- DROP STATEMENTS --\n\n")
        for drop in extracted_objects['drops']:
            output.append(drop['body'])
            output.append("\nGO\n\n")
    
    if extracted_objects['functions']:
        output.append("-- FUNCTIONS --\n\n")
        for func in extracted_objects['functions']:
            output.append(func['body'])
            output.append("\nGO\n\n")
    
    if extracted_objects['procedures']:
        output.append("-- PROCEDURES --\n\n")
        for proc in extracted_objects['procedures']:
            output.append(proc['body'])
            output.append("\nGO\n\n")
    
    return "".join(output)

# --------------------------
# Entry Point
# --------------------------

if __name__ == "__main__":
    try:
        config = load_environment_variables()
        if not generate_sync_script(config):
            print("\nFailed to generate sync script.")
    except ValueError as e:
        print(f"❌ Error: {e}")
        exit_code = 1
    else:
        exit_code = 0
    finally:
        input("\nPress Enter to exit...")
        exit(exit_code)
