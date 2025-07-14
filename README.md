
# DatabaseSyncTool

A Python-based automation tool to extract schema differences (functions, procedures, drop statements) between two SQL Server databases using `SqlPackage.exe`. It generates a filtered, cleaned SQL sync script using `DROP IF EXISTS` for safe deployments.

---

## 📦 Features

- Extracts DACPAC from a source SQL Server database.
- Generates schema synchronization script with optional `DropObjectsNotInSource=True`.
- Parses and filters the script to extract:
  - Stored procedures
  - User-defined functions
  - DROP statements
- Cleans and converts the script to use `DROP IF EXISTS` before creation.
- Outputs:
  - Full sync script
  - Filtered SQL objects
  - Final cleaned script

---

## 🛠 Requirements

- Python 3.7+
- [`sqlpackage.exe`](https://learn.microsoft.com/en-us/sql/tools/sqlpackage/sqlpackage-download)
- Environment variables (defined in `.env`)
- `python-dotenv` module  
  Install via:
  ```bash
  pip install python-dotenv
  ```

---

## ⚙️ Environment Setup

Create a `.env` file in the root directory with the following content:

```ini
# Database connection
SERVER=YourServerName
SQL_USERNAME=YourSQLUsername
SQL_PASSWORD=YourSQLPassword

# Database names
SOURCE_DB=YourSourceDatabase
TARGET_DB=YourTargetDatabase

# Path to SqlPackage.exe
SQL_PACKAGE_PATH="C:\Program Files\Microsoft SQL Server\160\DAC\bin\SqlPackage.exe"
```

---

## 🚀 How to Run

```bash
python vs2022_schema_compare.py
```

The following files will be generated in the `/output` directory:

- `SOURCE_DB.dacpac`
- `sync_script_main.sql` (raw sync script)
- `filtered_sync_script.sql` (filtered SQL objects)
- `final_script.sql` (cleaned version with DROP IF EXISTS)

---

## 🧊 Build as Executable

To create a standalone `.exe`:

1. Install PyInstaller:
   ```bash
   pip install pyinstaller
   ```

2. Run the following command:
   ```bash
   pyinstaller --name=DatabaseSyncTool .\vs2022_schema_compare.py
   ```

3. The executable will be located in the `dist/DatabaseSyncTool/` directory.

---

## 📂 Output Folder Structure

```
output/
├── SOURCE_DB.dacpac
├── sync_script_main.sql
├── filtered_sync_script.sql
└── final_script.sql
```

---

## 📃 License

MIT License

---

## 🙌 Credits

Developed by ammansoomro
Visit: ammansoomro.com
