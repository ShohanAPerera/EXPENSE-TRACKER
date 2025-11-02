import sqlite3
import oracledb
from datetime import datetime

# ---------- SQLite setup ----------
sqlite_db_path = r"D:\EXPENSE-TRACKER\instance\expenses.db"
sqlite_conn = sqlite3.connect(sqlite_db_path)
sqlite_cur = sqlite_conn.cursor()

# ---------- Oracle setup ----------
ORACLE_USER = "system"
ORACLE_PASS = "root"
ORACLE_DSN = "localhost:1521/xe"

oracle_conn = oracledb.connect(
    user=ORACLE_USER,
    password=ORACLE_PASS,
    dsn=ORACLE_DSN
)
oracle_cur = oracle_conn.cursor()

# ---------- Table-specific column mapping ----------
TABLE_COLUMN_MAPPINGS = {
    'expense': {
        'date': 'expense_date'  # Map SQLite 'date' to Oracle 'expense_date'
    },
    'saving': {
        'date': 'saving_date'   # Map SQLite 'date' to Oracle 'saving_date'
    }
    # category_budget doesn't need mapping as columns match
}

def get_oracle_columns_with_types(table_name):
    """Get column names and data types from existing Oracle table"""
    try:
        oracle_cur.execute("""
            SELECT column_name, data_type 
            FROM user_tab_columns 
            WHERE table_name = UPPER(:1)
            ORDER BY column_id
        """, [table_name])
        return oracle_cur.fetchall()
    except Exception as e:
        print(f"Error getting Oracle columns for {table_name}: {e}")
        return []

def convert_date_for_oracle(date_value):
    """Convert SQLite date string to Oracle DATE format"""
    if date_value is None:
        return None
    
    # If it's already a datetime object
    if isinstance(date_value, datetime):
        return date_value
    
    # If it's a string, handle SQLite datetime format with microseconds
    if isinstance(date_value, str):
        try:
            # Handle SQLite format with microseconds: 2025-10-31 11:52:40.309062
            if '.' in date_value:
                # Split to remove microseconds
                date_part = date_value.split('.')[0]
                return datetime.strptime(date_part, '%Y-%m-%d %H:%M:%S')
            else:
                # Try standard formats without microseconds
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d',
                    '%d-%m-%Y',
                    '%m/%d/%Y'
                ]
                for fmt in formats:
                    try:
                        return datetime.strptime(date_value, fmt)
                    except ValueError:
                        continue
        except Exception as e:
            print(f"Warning: Could not parse date '{date_value}': {e}")
    
    return date_value

def sync_table_data(table_name):
    """Sync data from SQLite to Oracle for a specific table"""
    print(f"\n📊 Syncing table: {table_name}")
    
    # Get SQLite columns
    sqlite_cur.execute(f'PRAGMA table_info("{table_name}")')
    sqlite_columns = [col[1].lower() for col in sqlite_cur.fetchall()]
    
    # Get Oracle columns with types
    oracle_columns_info = get_oracle_columns_with_types(table_name)
    oracle_columns = [col[0].lower() for col in oracle_columns_info]
    
    if not sqlite_columns:
        print(f"  ✗ No columns found in SQLite table")
        return
    
    if not oracle_columns:
        print(f"  ✗ Table {table_name} not found in Oracle")
        return
    
    print(f"  SQLite columns: {sqlite_columns}")
    print(f"  Oracle columns: {oracle_columns}")
    
    # Build column mapping using table-specific mappings
    column_mapping = []
    for sqlite_col in sqlite_columns:
        # Get table-specific mapping, fall back to same column name
        if table_name in TABLE_COLUMN_MAPPINGS and sqlite_col in TABLE_COLUMN_MAPPINGS[table_name]:
            oracle_col = TABLE_COLUMN_MAPPINGS[table_name][sqlite_col]
        else:
            oracle_col = sqlite_col
        
        if oracle_col in oracle_columns:
            column_mapping.append((sqlite_col, oracle_col))
        else:
            print(f"  ✗ Column '{sqlite_col}' -> '{oracle_col}' not found in Oracle table")
            return
    
    print(f"  Column mapping: {[f'{src} -> {dest}' for src, dest in column_mapping]}")
    
    # Fetch all rows from SQLite
    sqlite_cur.execute(f'SELECT * FROM "{table_name}"')
    rows = sqlite_cur.fetchall()
    
    if not rows:
        print("  No data to sync")
        return
    
    # Prepare insert SQL with proper placeholders
    oracle_column_names = [dest for _, dest in column_mapping]
    column_list = ", ".join(oracle_column_names)
    placeholders = ", ".join([f":{i+1}" for i in range(len(column_mapping))])
    insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
    
    print(f"  Rows to insert: {len(rows)}")
    
    try:
        # Clear existing data in Oracle table
        oracle_cur.execute(f"DELETE FROM {table_name}")
        print(f"  Cleared existing data from Oracle table")
        
        # Process and insert rows with date conversion
        successful_rows = 0
        for row in rows:
            try:
                processed_row = []
                for i, (value, (sqlite_col, oracle_col)) in enumerate(zip(row, column_mapping)):
                    # Find Oracle column type
                    oracle_col_type = None
                    for col_name, col_type in oracle_columns_info:
                        if col_name.lower() == oracle_col:
                            oracle_col_type = col_type
                            break
                    
                    # Convert date values for DATE columns
                    if oracle_col_type == 'DATE' and value is not None:
                        processed_value = convert_date_for_oracle(value)
                    else:
                        processed_value = value
                    
                    processed_row.append(processed_value)
                
                # Insert the processed row
                oracle_cur.execute(insert_sql, processed_row)
                successful_rows += 1
                
            except Exception as e:
                print(f"  Warning: Failed to insert row {row}: {e}")
                continue
        
        oracle_conn.commit()
        print(f"  ✓ {successful_rows}/{len(rows)} rows synced to Oracle")
        
    except Exception as e:
        print(f"  ✗ Error syncing data: {e}")
        oracle_conn.rollback()

# ---------- Main sync logic ----------
print("Starting SQLite to Oracle data sync...")

# Get all tables from SQLite
sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = sqlite_cur.fetchall()

for table_name_tuple in tables:
    table_name = table_name_tuple[0]
    sync_table_data(table_name)

# Close connections
sqlite_conn.close()
oracle_conn.close()
print("\n🎉 Data sync complete!")