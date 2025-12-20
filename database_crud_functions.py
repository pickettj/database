#!/usr/bin/env python3
"""
Library of functions for editing Eurasia database
Handles adding, updating, and deleting entries with automatic foreign key resolution
"""

import sqlite3
import os
import re
from datetime import datetime

# Import query functions for reuse
import sys
sys.path.insert(0, os.path.expanduser('~/Projects/database'))
from database_query_functions import database_path, database_info

# Local regex functions for case-insensitive search
def _regex_search_case_insensitive(pattern, string):
    """Enable regex search in SQLite with case-insensitive matching"""
    if not isinstance(string, str):
        return False
    try:
        return re.search(pattern, string, flags=re.IGNORECASE) is not None
    except Exception as e:
        return False

def _register_regex_local(conn):
    """Register case-insensitive regex function with SQLite connection"""
    conn.create_function("REGEXP", 2, _regex_search_case_insensitive)

"""
Setting up the database paths
"""

# Set home directory path
hdir = os.path.expanduser('~')

dh_path = '/Dropbox/Active_Directories/Digital_Humanities/'
inbox_path = os.path.join(hdir, 'Dropbox/Active_Directories/Inbox')

# Use database_path from query functions for consistency
# database_path already defined from import

# Verify database exists
if not os.path.exists(database_path):
    raise FileNotFoundError(f"Database file not found at: {database_path}")

print(f"✅ Database editing library loaded")
print(f"📁 Database: {database_path}")

"""
Core Helper Functions
"""

# System fields to skip (auto-generated or legacy timestamps)
SYSTEM_FIELDS = {
    'UID',                      # Primary key (auto-generated)
    'Created_At',               # Standard timestamp fields
    'Modified_At',
    'Timestamp',                # Legacy timestamp fields (preserve but don't prompt)
    'Timestamp_Created',
    'Timestamp_Modified',
    'Time_Stamp',               # Alternative underscore variant
    'Date_Created',             # Other possible variants
    'Date_Modified'
}

# Table-specific fields to ignore during data entry
# Format: {'table_name': {'field1', 'field2', ...}}
# Add fields here that should be auto-generated, computed, or not manually entered
# These fields will not appear in add_entry() or update_entry() prompts
TABLE_SPECIFIC_IGNORES = {
    'bibliography': {
        'Citation',        # Auto-generated or computed field
        'Short_Citation'   # Auto-generated or computed field
    },
    # Add more tables as needed:
    # 'gazetteer': {'computed_field', 'legacy_field'},
    # 'prosopography': {'auto_generated_name'},
}


def _should_skip_field(table_name, field_name):
    """
    Determine if a field should be skipped during data entry.
    
    Args:
        table_name: Name of the table
        field_name: Name of the field
    
    Returns:
        bool: True if field should be skipped, False otherwise
    """
    # Skip global system fields
    if field_name in SYSTEM_FIELDS:
        return True
    
    # Skip table-specific ignored fields
    if table_name in TABLE_SPECIFIC_IGNORES:
        if field_name in TABLE_SPECIFIC_IGNORES[table_name]:
            return True
    
    return False


def _get_all_tables():
    """Get list of all tables in the database"""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT name 
        FROM sqlite_master 
        WHERE type='table'
        ORDER BY name;
    """)
    
    tables = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return tables


def _get_table_schema(table_name):
    """
    Get complete schema information for a table
    
    Returns:
        dict with 'columns' and 'foreign_keys' keys
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    # Get column information
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    # Structure: (cid, name, type, notnull, default_val, pk)
    
    # Get foreign key information
    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
    foreign_keys = cursor.fetchall()
    # Structure: (id, seq, ref_table, from_col, to_col, on_update, on_delete, match)
    
    cursor.close()
    conn.close()
    
    return {
        'columns': columns,
        'foreign_keys': foreign_keys
    }


def _get_next_uid(table_name):
    """Get the next available UID for a table"""
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT MAX(UID) FROM {table_name}")
    max_uid = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    return (max_uid or 0) + 1


def _get_fk_info(table_name, fk_column):
    """
    Get information about a foreign key relationship
    
    Returns:
        dict with 'ref_table', 'ref_column', 'fk_column' or None
    """
    schema = _get_table_schema(table_name)
    
    for fk in schema['foreign_keys']:
        # fk structure: (id, seq, ref_table, from_col, to_col, on_update, on_delete, match)
        if fk[3] == fk_column:  # fk[3] is the 'from' column
            return {
                'ref_table': fk[2],      # Which table it references
                'ref_column': fk[4],     # Which column (usually 'UID')
                'fk_column': fk[3]       # Your column name
            }
    
    return None


def _is_column_required(column_info):
    """Check if a column is required (NOT NULL and not auto-generated)"""
    # column_info structure: (cid, name, type, notnull, default_val, pk)
    col_name = column_info[1]
    not_null = column_info[3]
    is_pk = column_info[5]
    
    # Skip system fields (UID, timestamps, etc.)
    if is_pk or col_name in SYSTEM_FIELDS:
        return False
    
    return not_null == 1


"""
Foreign Key Resolution Functions
"""

# Columns to skip when searching (non-searchable or system fields)
SKIP_SEARCH_COLUMNS = SYSTEM_FIELDS | {
    'Latitude', 'Longitude',
    'Date_Birth', 'Date_Death', 'Date_Start', 'Date_End',
    'Date_Pub_Greg', 'Date_Pub_Hij'
}


def _normalize_search_term(search_term):
    """
    Normalize search term for better matching
    - Escapes special regex characters
    - Makes apostrophes flexible to match different Unicode variants
    """
    # Escape special regex characters (except apostrophes, we'll handle those specially)
    # Special regex chars: . ^ $ * + ? { } [ ] \ | ( )
    escaped = re.sub(r'([.^$*+?{}\[\]\\|()])', r'\\\1', search_term)
    
    # Replace any apostrophe-like character with alternation pattern
    # Using (?:...) non-capturing group with | alternation works better in SQLite
    # This handles: ' (U+0027), ' (U+2019), ʻ (U+02BB), ` (U+0060)
    apostrophe_pattern = r"(?:'|'|ʻ|`)"
    normalized = re.sub(r"['ʻ`'']", apostrophe_pattern, escaped)
    
    return normalized


def _search_in_table(ref_table, search_term, max_results=20):
    """
    Search ALL text fields in a table for matches
    
    Args:
        ref_table: Table to search in
        search_term: Regex pattern to search for (will be normalized for better matching)
        max_results: Maximum number of results to return
    
    Returns:
        List of dicts with all column values
    """
    conn = sqlite3.connect(database_path)
    _register_regex_local(conn)
    cursor = conn.cursor()
    
    # Normalize search term for better matching (handles apostrophes, escapes special chars)
    normalized_term = _normalize_search_term(search_term)
    
    # Get all columns
    cursor.execute(f"PRAGMA table_info({ref_table})")
    columns = cursor.fetchall()
    
    # Filter to searchable TEXT columns
    text_columns = []
    all_columns = []
    
    for col in columns:
        col_name = col[1]
        col_type = col[2]
        all_columns.append(col_name)
        
        # Skip non-searchable columns
        if col_name in SKIP_SEARCH_COLUMNS:
            continue
        
        # Include TEXT fields
        if col_type in ['TEXT', 'VARCHAR', '']:
            text_columns.append(col_name)
    
    if not text_columns:
        print(f"⚠️  No searchable text columns found in {ref_table}")
        print(f"   Available columns: {all_columns}")
        cursor.close()
        conn.close()
        return []
    
    # Build OR query across all text fields
    where_clauses = [f"{col} REGEXP ?" for col in text_columns]
    where_sql = " OR ".join(where_clauses)
    
    query = f"""
        SELECT {', '.join(all_columns)}
        FROM {ref_table}
        WHERE {where_sql}
        LIMIT ?
    """
    
    # Execute with normalized search term repeated for each column
    params = tuple([normalized_term] * len(text_columns) + [max_results])
    
    try:
        cursor.execute(query, params)
        results = cursor.fetchall()
    except Exception as e:
        print(f"❌ Search error in {ref_table}: {e}")
        print(f"   Query: {query}")
        print(f"   Normalized term: {normalized_term}")
        print(f"   Text columns: {text_columns}")
        results = []
    
    cursor.close()
    conn.close()
    
    # Convert to list of dicts
    return [dict(zip(all_columns, row)) for row in results]


def _display_fk_results(results, ref_table):
    """
    Display search results in a user-friendly format
    
    Tries to intelligently pick the most relevant fields to show
    """
    # Priority fields to show first (if they exist)
    priority_fields = [
        'Name_Arabic', 'Title', 'Nickname', 'Acronym', 'Term', 
        'Author', 'Name_English', 'Name_Foreign', 'Name_Latin',
        'Name_Strict_Translit', 'Location_Name_Arabic', 'Translation'
    ]
    
    for i, row in enumerate(results, 1):
        # Find the main field to display
        main_field = None
        main_value = None
        
        for field in priority_fields:
            if field in row and row[field]:
                main_field = field
                main_value = row[field]
                break
        
        # Fallback to UID if nothing else
        if not main_value:
            main_value = f"Entry {row.get('UID', '?')}"
        
        print(f"{i}. {main_value}")
        
        # Show other relevant non-null fields (max 3 additional)
        shown = 0
        for key, val in row.items():
            if (key != 'UID' and key != main_field and val and 
                not key.startswith('_') and key not in SKIP_SEARCH_COLUMNS):
                print(f"   {key}: {val}")
                shown += 1
                if shown >= 3:
                    break


def _resolve_foreign_key(table_name, fk_column):
    """
    Interactive foreign key resolution
    
    Args:
        table_name: The table being edited
        fk_column: The foreign key column name
    
    Returns:
        int: UID of selected entry, or None if skipped
    """
    # Get FK relationship info
    fk_info = _get_fk_info(table_name, fk_column)
    
    if not fk_info:
        print(f"⚠️  {fk_column} is not a foreign key")
        return None
    
    ref_table = fk_info['ref_table']
    
    print(f"\n🔗 Foreign Key: {fk_column}")
    print(f"   References: {ref_table}.{fk_info['ref_column']}")
    print("-" * 60)
    
    while True:
        search_term = input(f"Search {ref_table} (or press Enter to skip): ").strip()
        
        if not search_term:
            print("   ⏭️  Skipped (NULL)")
            return None
        
        # Search the referenced table
        results = _search_in_table(ref_table, search_term)
        
        if not results:
            print(f"   ❌ No matches found for '{search_term}'")
            print(f"   💡 Tip: Try simpler terms (e.g., 'uzbek' instead of 'O'z')")
            retry = input("   Try again? (y/n): ").strip().lower()
            if retry != 'y':
                return None
            continue
        
        # Display results
        print(f"\n   Found {len(results)} results:")
        _display_fk_results(results, ref_table)
        
        # Get user selection
        choice = input(f"\nSelect (1-{len(results)}), search again (s), or skip (Enter): ").strip().lower()
        
        if choice == 's':
            continue  # Search again
        elif not choice:
            print("   ⏭️  Skipped (NULL)")
            return None
        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(results):
                selected_uid = results[idx]['UID']
                # Show what was selected
                main_display = None
                for field in ['Name_Arabic', 'Title', 'Nickname', 'Acronym', 'Author']:
                    if field in results[idx] and results[idx][field]:
                        main_display = results[idx][field]
                        break
                print(f"   ✅ Selected: {main_display or 'UID ' + str(selected_uid)}")
                return selected_uid
            else:
                print(f"   ❌ Invalid selection. Choose 1-{len(results)}")
        else:
            print("   ❌ Invalid input")


"""
Main Entry Functions
"""

def add_entry(table_name=None):
    """
    Add a new entry to any table with interactive prompts.
    Automatically handles foreign key resolution.
    
    Args:
        table_name (str, optional): Name of table to add to. 
                                   If None, displays menu of all tables.
    
    Returns:
        int: UID of newly inserted row, or None if cancelled
    
    Examples:
        add_entry()                    # Shows table menu
        add_entry('bibliography')      # Add to bibliography directly
    """
    
    # If no table specified, show menu
    if table_name is None:
        tables = _get_all_tables()
        
        print("\n" + "=" * 70)
        print("📋 SELECT TABLE TO ADD ENTRY")
        print("=" * 70)
        
        for i, table in enumerate(tables, 1):
            print(f"{i:2d}. {table}")
        
        print("=" * 70)
        
        choice = input("\nSelect table (1-{}) or 'q' to quit: ".format(len(tables))).strip()
        
        if choice.lower() == 'q':
            print("Cancelled.")
            return None
        
        if not choice.isdigit() or not (1 <= int(choice) <= len(tables)):
            print("❌ Invalid selection")
            return None
        
        table_name = tables[int(choice) - 1]
    
    # Verify table exists
    if table_name not in _get_all_tables():
        print(f"❌ Table '{table_name}' does not exist")
        return None
    
    print("\n" + "=" * 70)
    print(f"📝 ADDING ENTRY TO: {table_name}")
    print("=" * 70)
    
    # Get table schema
    schema = _get_table_schema(table_name)
    
    # Generate next UID
    next_uid = _get_next_uid(table_name)
    print(f"🔢 Auto-generating UID: {next_uid}")
    print("=" * 70)
    
    # Separate columns into simple and FK columns
    fk_column_names = {fk[3] for fk in schema['foreign_keys']}
    
    entry_data = {'UID': next_uid}
    
    # First pass: collect simple fields
    print("\n📝 SIMPLE FIELDS")
    print("-" * 60)
    
    for col in schema['columns']:
        col_name = col[1]
        col_type = col[2]
        is_required = _is_column_required(col)
        
        # Skip system fields, table-specific ignores, and FK columns (handle FKs later)
        if _should_skip_field(table_name, col_name) or col_name in fk_column_names:
            continue
        
        # Prompt for input
        required_marker = " (required)" if is_required else " (optional)"
        value = input(f"{col_name}{required_marker}: ").strip()
        
        # Handle empty input
        if not value:
            if is_required:
                print(f"   ⚠️  Warning: {col_name} is required but left empty")
            entry_data[col_name] = None
        else:
            entry_data[col_name] = value
    
    # Second pass: resolve foreign keys
    if fk_column_names:
        print("\n🔗 FOREIGN KEY FIELDS")
        print("-" * 60)
        
        for col in schema['columns']:
            col_name = col[1]
            
            # Skip table-specific ignores even for FK fields
            if _should_skip_field(table_name, col_name):
                continue
            
            if col_name in fk_column_names:
                fk_uid = _resolve_foreign_key(table_name, col_name)
                entry_data[col_name] = fk_uid
    
    # Review and confirm
    print("\n" + "=" * 70)
    print("📋 REVIEW ENTRY")
    print("=" * 70)
    
    for key, value in entry_data.items():
        if value is not None:
            display_val = str(value)[:60] + "..." if len(str(value)) > 60 else value
            print(f"  • {key}: {display_val}")
        else:
            print(f"  • {key}: NULL")
    
    print("=" * 70)
    
    # Confirm insertion
    confirm = input("\nInsert this entry? (y/n/edit): ").strip().lower()
    
    if confirm == 'edit':
        print("⚠️  Edit functionality not yet implemented")
        return None
    elif confirm != 'y':
        print("❌ Cancelled")
        return None
    
    # Perform insertion
    try:
        conn = sqlite3.connect(database_path)
        conn.execute("PRAGMA foreign_keys = ON")  # Enforce FK constraints
        cursor = conn.cursor()
        
        # Build INSERT query
        columns = ', '.join(entry_data.keys())
        placeholders = ', '.join(['?' for _ in entry_data])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        cursor.execute(query, list(entry_data.values()))
        conn.commit()
        
        print(f"\n✅ Successfully inserted entry with UID: {next_uid}")
        
        cursor.close()
        conn.close()
        
        return next_uid
        
    except sqlite3.IntegrityError as e:
        print(f"\n❌ Database constraint error: {e}")
        print("   (This usually means a required field was NULL or a FK was invalid)")
        conn.rollback()
        conn.close()
        return None
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        conn.rollback()
        conn.close()
        return None


def update_entry(table_name=None, uid=None, field_name=None):
    """
    Update an existing entry with comprehensive safety checks.
    Allows updating multiple fields in sequence.
    
    Args:
        table_name (str, optional): Name of table containing the entry. 
                                   If None, displays menu of all tables.
        uid (int, optional): UID of entry to update.
                            If None, performs search to find entry.
        field_name (str, optional): Specific field to update.
                                   If None, prompts for field selection.
    
    Returns:
        bool: True if update successful, False if cancelled or failed
    
    Examples:
        update_entry()                              # Interactive: select table, search, update fields
        update_entry('bibliography')                # Select table, then search, then update
        update_entry('bibliography', 1523)          # Select entry, then update fields
        update_entry('bibliography', 1523, 'Title') # Update specific field directly
    """
    
    # If no table specified, show menu
    if table_name is None:
        tables = _get_all_tables()
        
        print("\n" + "=" * 70)
        print("✏️  SELECT TABLE TO UPDATE")
        print("=" * 70)
        
        for i, table in enumerate(tables, 1):
            print(f"{i:2d}. {table}")
        
        print("=" * 70)
        
        choice = input("\nSelect table (1-{}) or 'q' to quit: ".format(len(tables))).strip()
        
        if choice.lower() == 'q':
            print("Cancelled.")
            return False
        
        if not choice.isdigit() or not (1 <= int(choice) <= len(tables)):
            print("❌ Invalid selection")
            return False
        
        table_name = tables[int(choice) - 1]
    
    # Verify table exists
    if table_name not in _get_all_tables():
        print(f"❌ Table '{table_name}' does not exist")
        return False
    
    # If no UID specified, search for it
    if uid is None:
        print(f"\n🔍 Search {table_name} to find entry to update")
        print("-" * 60)
        
        while True:
            search_term = input("Search (all fields): ").strip()
            
            if not search_term:
                print("Search cancelled.")
                return False
            
            # Search across all fields
            results = _search_in_table(table_name, search_term, max_results=20)
            
            if not results:
                print(f"   ❌ No matches found for '{search_term}'")
                retry = input("   Try again? (y/n): ").strip().lower()
                if retry != 'y':
                    return False
                continue
            
            # Display results
            print(f"\n   Found {len(results)} results:")
            _display_fk_results(results, table_name)
            
            # Get user selection
            choice = input(f"\nSelect entry to update (1-{len(results)}), search again (s), or cancel (c): ").strip().lower()
            
            if choice == 's':
                continue
            elif choice == 'c':
                print("Cancelled.")
                return False
            elif choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(results):
                    uid = results[idx]['UID']
                    break
                else:
                    print(f"   ❌ Invalid selection. Choose 1-{len(results)}")
            else:
                print("   ❌ Invalid input")
    
    # Now we have table_name and uid - get current record
    conn = sqlite3.connect(database_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    
    try:
        # Get the current record
        cursor.execute(f"SELECT * FROM {table_name} WHERE UID = ?", (uid,))
        record = cursor.fetchone()
        
        if not record:
            print(f"❌ No record found with UID {uid} in {table_name}")
            cursor.close()
            conn.close()
            return False
        
        # Get column info
        schema = _get_table_schema(table_name)
        columns = schema['columns']
        column_names = [col[1] for col in columns]
        fk_columns = {fk[3] for fk in schema['foreign_keys']}
        
        # Create dict of current record
        current_record = dict(zip(column_names, record))
        
        # Display current record
        print("\n" + "=" * 70)
        print(f"✏️  UPDATING ENTRY: {table_name} (UID: {uid})")
        print("=" * 70)
        print("\nCurrent values:")
        for key, value in current_record.items():
            if key not in SYSTEM_FIELDS:
                display_val = str(value)[:60] + "..." if value and len(str(value)) > 60 else value
                print(f"  • {key}: {display_val}")
        print("=" * 70)
        
        # Track updates
        updates = {}
        any_updates = False
        
        # If specific field provided, update just that field
        if field_name is not None:
            if field_name not in column_names:
                print(f"❌ Field '{field_name}' does not exist in {table_name}")
                cursor.close()
                conn.close()
                return False
            
            if _should_skip_field(table_name, field_name):
                print(f"❌ Cannot update system or ignored field '{field_name}'")
                cursor.close()
                conn.close()
                return False
            
            # Update this specific field
            updated_value = _update_single_field(table_name, field_name, current_record[field_name], 
                                                 field_name in fk_columns, cursor)
            if updated_value is not None or updated_value != current_record[field_name]:
                updates[field_name] = updated_value
                any_updates = True
        else:
            # Interactive field selection - keep prompting until done
            print("\n💡 Select fields to update (you can update multiple fields)")
            print("   Enter 'done' when finished, or 'cancel' to abort\n")
            
            while True:
                # Show available fields (exclude system fields)
                updatable_fields = [col[1] for col in columns 
                                   if not _should_skip_field(table_name, col[1])]
                
                print("\nAvailable fields:")
                for i, field in enumerate(updatable_fields, 1):
                    current_val = current_record[field]
                    display_val = str(current_val)[:40] + "..." if current_val and len(str(current_val)) > 40 else current_val
                    updated_marker = " ✏️ [UPDATED]" if field in updates else ""
                    print(f"{i:2d}. {field}: {display_val}{updated_marker}")
                
                print("\n" + "-" * 60)
                choice = input("Select field to update (1-{}), 'done', or 'cancel': ".format(len(updatable_fields))).strip().lower()
                
                if choice == 'done':
                    break
                elif choice == 'cancel':
                    print("Update cancelled.")
                    cursor.close()
                    conn.close()
                    return False
                elif choice.isdigit() and 1 <= int(choice) <= len(updatable_fields):
                    selected_field = updatable_fields[int(choice) - 1]
                    
                    # Update this field
                    updated_value = _update_single_field(table_name, selected_field, 
                                                         current_record[selected_field],
                                                         selected_field in fk_columns, cursor)
                    
                    # Check if value actually changed
                    if updated_value != current_record[selected_field]:
                        updates[selected_field] = updated_value
                        current_record[selected_field] = updated_value  # Update for display
                        any_updates = True
                        print(f"   ✅ {selected_field} updated")
                    else:
                        print(f"   ℹ️  {selected_field} unchanged")
                else:
                    print("   ❌ Invalid selection")
        
        # If no updates made, exit
        if not any_updates:
            print("\n⚠️  No changes made - update cancelled")
            cursor.close()
            conn.close()
            return False
        
        # Review changes and confirm
        print("\n" + "=" * 70)
        print("📋 REVIEW CHANGES")
        print("=" * 70)
        print(f"\nTable: {table_name}")
        print(f"UID: {uid}\n")
        print("Changes to be made:")
        
        for field, new_value in updates.items():
            old_value = record[column_names.index(field)]
            old_display = str(old_value)[:40] + "..." if old_value and len(str(old_value)) > 40 else old_value
            new_display = str(new_value)[:40] + "..." if new_value and len(str(new_value)) > 40 else new_value
            
            print(f"  • {field}:")
            print(f"      Old: {old_display}")
            print(f"      New: {new_display}")
        
        print("\n" + "=" * 70)
        confirm = input("\nApply these changes? (y/n): ").strip().lower()
        
        if confirm != 'y':
            print("❌ Update cancelled")
            cursor.close()
            conn.close()
            return False
        
        # Perform the update
        set_clauses = [f"{field} = ?" for field in updates.keys()]
        set_sql = ", ".join(set_clauses)
        values = list(updates.values()) + [uid]
        
        update_query = f"UPDATE {table_name} SET {set_sql} WHERE UID = ?"
        cursor.execute(update_query, values)
        conn.commit()
        
        print(f"\n✅ Entry UID {uid} updated in {table_name}")
        print(f"   {len(updates)} field(s) modified")
        
        cursor.close()
        conn.close()
        
        return True
        
    except sqlite3.IntegrityError as e:
        print(f"\n❌ Cannot update: {e}")
        print("   (Constraint violation - check foreign keys and required fields)")
        conn.rollback()
        conn.close()
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        conn.rollback()
        conn.close()
        return False


def _update_single_field(table_name, field_name, current_value, is_foreign_key, cursor):
    """
    Helper function to update a single field with appropriate handling for FKs.
    
    Args:
        table_name: Table being updated
        field_name: Field to update
        current_value: Current value of the field
        is_foreign_key: Whether this is a foreign key field
        cursor: Database cursor for FK lookups
    
    Returns:
        New value for the field (or current_value if unchanged)
    """
    print(f"\n✏️  Updating: {field_name}")
    print(f"   Current value: {current_value}")
    
    if is_foreign_key:
        # Handle foreign key update
        print(f"   (This is a foreign key field)")
        
        change = input("   Change this foreign key? (y/n/null): ").strip().lower()
        
        if change == 'n':
            return current_value
        elif change == 'null':
            return None
        elif change == 'y':
            # Use FK resolution
            new_uid = _resolve_foreign_key(table_name, field_name)
            return new_uid if new_uid is not None else current_value
        else:
            print("   Invalid choice, keeping current value")
            return current_value
    else:
        # Handle regular field update
        print("   Enter new value (or press Enter to keep current, 'null' to set NULL):")
        new_value = input("   > ").strip()
        
        if not new_value:
            return current_value
        elif new_value.lower() == 'null':
            return None
        else:
            return new_value


def delete_entry(table_name=None, uid=None):
    """
    Delete an entry from a table with comprehensive safety checks.
    
    Args:
        table_name (str, optional): Name of table to delete from. 
                                   If None, displays menu of all tables.
        uid (int, optional): UID of entry to delete.
                            If None, performs search to find entry.
    
    Returns:
        bool: True if deletion successful, False if cancelled or failed
    
    Examples:
        delete_entry()                      # Interactive: select table, search, confirm
        delete_entry('bibliography')        # Select table, then search
        delete_entry('bibliography', 1523)  # Direct deletion with confirmation
    """
    
    # If no table specified, show menu
    if table_name is None:
        tables = _get_all_tables()
        
        print("\n" + "=" * 70)
        print("🗑️  SELECT TABLE TO DELETE FROM")
        print("=" * 70)
        
        for i, table in enumerate(tables, 1):
            print(f"{i:2d}. {table}")
        
        print("=" * 70)
        
        choice = input("\nSelect table (1-{}) or 'q' to quit: ".format(len(tables))).strip()
        
        if choice.lower() == 'q':
            print("Cancelled.")
            return False
        
        if not choice.isdigit() or not (1 <= int(choice) <= len(tables)):
            print("❌ Invalid selection")
            return False
        
        table_name = tables[int(choice) - 1]
    
    # Verify table exists
    if table_name not in _get_all_tables():
        print(f"❌ Table '{table_name}' does not exist")
        return False
    
    # If no UID specified, search for it
    if uid is None:
        print(f"\n🔍 Search {table_name} to find entry to delete")
        print("-" * 60)
        
        while True:
            search_term = input("Search (all fields): ").strip()
            
            if not search_term:
                print("Search cancelled.")
                return False
            
            # Search across all fields
            results = _search_in_table(table_name, search_term, max_results=20)
            
            if not results:
                print(f"   ❌ No matches found for '{search_term}'")
                retry = input("   Try again? (y/n): ").strip().lower()
                if retry != 'y':
                    return False
                continue
            
            # Display results
            print(f"\n   Found {len(results)} results:")
            _display_fk_results(results, table_name)
            
            # Get user selection
            choice = input(f"\nSelect entry to delete (1-{len(results)}), search again (s), or cancel (c): ").strip().lower()
            
            if choice == 's':
                continue
            elif choice == 'c':
                print("Cancelled.")
                return False
            elif choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(results):
                    uid = results[idx]['UID']
                    break
                else:
                    print(f"   ❌ Invalid selection. Choose 1-{len(results)}")
            else:
                print("   ❌ Invalid input")
    
    # Now we have table_name and uid - get full record details
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    try:
        # Get the record
        cursor.execute(f"SELECT * FROM {table_name} WHERE UID = ?", (uid,))
        record = cursor.fetchone()
        
        if not record:
            print(f"❌ No record found with UID {uid} in {table_name}")
            cursor.close()
            conn.close()
            return False
        
        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        # Create dict of record
        record_dict = dict(zip(column_names, record))
        
        # Check for related records (records that reference this UID via FK)
        related_records = []
        all_tables = _get_all_tables()
        
        for other_table in all_tables:
            if other_table == table_name:
                continue
            
            # Get foreign keys in the other table
            cursor.execute(f"PRAGMA foreign_key_list({other_table})")
            fks = cursor.fetchall()
            
            for fk in fks:
                # fk structure: (id, seq, ref_table, from_col, to_col, on_update, on_delete, match)
                if fk[2] == table_name:  # References our table
                    # Check if any records in other_table reference this UID
                    fk_column = fk[3]
                    cursor.execute(f"SELECT COUNT(*) FROM {other_table} WHERE {fk_column} = ?", (uid,))
                    count = cursor.fetchone()[0]
                    
                    if count > 0:
                        related_records.append({
                            'table': other_table,
                            'column': fk_column,
                            'count': count
                        })
        
        # Display deletion warning with full details
        print("\n" + "=" * 70)
        print("⚠️  DELETION WARNING")
        print("=" * 70)
        print("You are about to PERMANENTLY delete:\n")
        print(f"Table: {table_name}")
        print(f"UID: {uid}\n")
        print("Fields:")
        
        # Display all non-null fields
        for key, value in record_dict.items():
            if value is not None and key not in SYSTEM_FIELDS:
                # Truncate long values
                display_val = str(value)[:60] + "..." if len(str(value)) > 60 else value
                
                # Try to resolve FK values to show meaningful info
                schema = _get_table_schema(table_name)
                fk_info = None
                for fk in schema['foreign_keys']:
                    if fk[3] == key:  # This is a FK column
                        fk_info = {'ref_table': fk[2], 'ref_column': fk[4]}
                        break
                
                if fk_info:
                    # Try to get display name from referenced table
                    try:
                        cursor.execute(f"SELECT * FROM {fk_info['ref_table']} WHERE UID = ?", (value,))
                        ref_record = cursor.fetchone()
                        if ref_record:
                            cursor.execute(f"PRAGMA table_info({fk_info['ref_table']})")
                            ref_columns = [col[1] for col in cursor.fetchall()]
                            ref_dict = dict(zip(ref_columns, ref_record))
                            
                            # Find a good display field
                            display_field = None
                            for field in ['Name_Arabic', 'Title', 'Nickname', 'Acronym', 'Author']:
                                if field in ref_dict and ref_dict[field]:
                                    display_field = ref_dict[field]
                                    break
                            
                            if display_field:
                                print(f"  • {key}: {value} ({display_field})")
                            else:
                                print(f"  • {key}: {value}")
                    except:
                        print(f"  • {key}: {display_val}")
                else:
                    print(f"  • {key}: {display_val}")
        
        # Display related records warning
        if related_records:
            print("\n⚠️  RELATED RECORDS:")
            for rel in related_records:
                print(f"  • {rel['count']} record(s) in '{rel['table']}.{rel['column']}' reference this entry")
            print("\nThese records will have NULL values after deletion.")
        
        print("\n" + "=" * 70)
        print("⚠️  THIS CANNOT BE UNDONE")
        print("=" * 70)
        
        # Require typing DELETE to confirm
        confirm = input("\nTo confirm deletion, type: DELETE\n> ").strip()
        
        if confirm != "DELETE":
            print("❌ Deletion cancelled (confirmation did not match)")
            cursor.close()
            conn.close()
            return False
        
        # Perform the deletion
        conn.execute("PRAGMA foreign_keys = ON")  # Ensure FK constraints are checked
        cursor.execute(f"DELETE FROM {table_name} WHERE UID = ?", (uid,))
        conn.commit()
        
        print(f"\n✅ Entry UID {uid} deleted from {table_name}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except sqlite3.IntegrityError as e:
        print(f"\n❌ Cannot delete: {e}")
        print("   (Foreign key constraint prevents deletion)")
        conn.rollback()
        conn.close()
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        conn.rollback()
        conn.close()
        return False


"""
Display available functions on load
"""

print("\n💡 Available functions:")
print("   • add_entry(table_name=None) - Add new entry to database")
print("   • update_entry(table_name=None, uid=None, field_name=None) - Update existing entry")
print("   • delete_entry(table_name=None, uid=None) - Delete entry with safety checks")
print("   • new_lex(new_term=None) - Streamlined lexicon entry with definition")
print("\n📖 Quick start: add_entry() | update_entry() | delete_entry() | new_lex()")


"""
Specialized Entry Functions
"""

def new_lex(new_term=None):
    """
    Streamlined function for creating new lexicon entries with definitions.
    Handles the complete workflow: create lexicon entry → add definition(s) → link to source.
    
    Args:
        new_term (str, optional): The term to add. If None, prompts user.
    
    Returns:
        int: UID of newly created lexicon entry, or None if cancelled
    
    Examples:
        new_lex()                    # Interactive: prompts for term
        new_lex('باج')               # Creates entry with term 'باج'
    """
    
    conn = sqlite3.connect(database_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    
    try:
        # Step 1: Create lexicon entry
        print("\n" + "=" * 70)
        print("📚 NEW LEXICON ENTRY")
        print("=" * 70)
        
        # Get or prompt for term
        if new_term is None:
            new_term = input("\nNew lexicon item: ").strip()
            if not new_term:
                print("❌ Term cannot be empty")
                cursor.close()
                conn.close()
                return None
        else:
            print(f"\nNew lexicon item: {new_term}")
        
        # Get next UID for lexicon
        lex_uid = _get_next_uid('lexicon')
        
        # Collect lexicon fields
        print("\n📝 Lexicon Fields")
        print("-" * 60)
        
        emic_term = input("Emic_Term: ").strip() or None
        transliteration = input("Transliteration: ").strip() or None
        translation = input("Translation: ").strip() or None
        
        # Insert lexicon entry
        cursor.execute("""
            INSERT INTO lexicon (UID, Term, Emic_Term, Transliteration, Translation)
            VALUES (?, ?, ?, ?, ?)
        """, (lex_uid, new_term, emic_term, transliteration, translation))
        
        print(f"\n✅ Lexicon entry created (UID: {lex_uid})")
        
        # Step 2: Add definition(s)
        while True:
            print("\n" + "=" * 70)
            print("📖 NEW DEFINITION")
            print("=" * 70)
            
            # Get next UID for definition
            def_uid = _get_next_uid('definitions')
            
            # Select definition type
            print("\nDefinition Type:")
            print("1. definition")
            print("2. example")
            print("3. note")
            print("4. reference")
            
            type_choice = input("\nSelect type (1-4): ").strip()
            type_map = {'1': 'definition', '2': 'example', '3': 'note', '4': 'reference'}
            def_type = type_map.get(type_choice, 'definition')
            
            # Get definition text
            definition_text = input(f"\nDefinition ({def_type}): ").strip()
            if not definition_text:
                print("⚠️  Definition cannot be empty, skipping...")
                break
            
            # Get Source_ID (with search capability)
            print("\n📚 Source (bibliography)")
            source_id = _get_source_id_for_definition(cursor)
            
            if source_id is None:
                print("⚠️  No source selected, definition will have NULL source")
            
            # Get Page_No
            page_no = input("\nPage_No: ").strip() or None
            
            # Get Specificity (tokenized selection)
            specificity = _get_specificity_selection(cursor)
            
            # Get Notes
            notes = input("\nNotes (optional): ").strip() or None
            
            # Insert definition
            cursor.execute("""
                INSERT INTO definitions (UID, Lexicon_ID, Type, Definition, Source_ID, Page_No, Specificity, Notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (def_uid, lex_uid, def_type, definition_text, source_id, page_no, specificity, notes))
            
            print(f"\n✅ Definition added (UID: {def_uid})")
            
            # Ask if user wants to add another definition
            another = input("\nAdd another definition for this term? (y/n): ").strip().lower()
            if another != 'y':
                break
        
        # Commit all changes
        conn.commit()
        
        print("\n" + "=" * 70)
        print(f"✅ Lexicon entry complete! (Lexicon UID: {lex_uid})")
        print("=" * 70)
        
        cursor.close()
        conn.close()
        
        return lex_uid
        
    except Exception as e:
        print(f"\n❌ Error creating lexicon entry: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return None


def _get_source_id_for_definition(cursor):
    """
    Helper function to get Source_ID for a definition.
    Accepts direct UID or searches bibliography by Author/Title/Gloss.
    
    Args:
        cursor: Database cursor
    
    Returns:
        int: Source UID, or None if skipped
    """
    while True:
        source_input = input("Source_ID (integer UID, search term, or Enter to skip): ").strip()
        
        if not source_input:
            return None
        
        # If it's an integer, use it directly
        if source_input.isdigit():
            source_uid = int(source_input)
            # Verify it exists
            cursor.execute("SELECT UID FROM bibliography WHERE UID = ?", (source_uid,))
            if cursor.fetchone():
                return source_uid
            else:
                print(f"   ❌ No bibliography entry with UID {source_uid}")
                continue
        
        # Otherwise, search bibliography
        conn_temp = sqlite3.connect(database_path)
        _register_regex_local(conn_temp)
        cursor_temp = conn_temp.cursor()
        
        normalized_term = _normalize_search_term(source_input)
        
        cursor_temp.execute("""
            SELECT UID, Author, Title, Gloss
            FROM bibliography
            WHERE Author REGEXP ? OR Title REGEXP ? OR Gloss REGEXP ?
            LIMIT 20
        """, (normalized_term, normalized_term, normalized_term))
        
        results = cursor_temp.fetchall()
        cursor_temp.close()
        conn_temp.close()
        
        if not results:
            print(f"   ❌ No matches found for '{source_input}'")
            retry = input("   Try again? (y/n): ").strip().lower()
            if retry != 'y':
                return None
            continue
        
        # Display results (truncated for single line)
        print(f"\n   Found {len(results)} sources:")
        for i, (uid, author, title, gloss) in enumerate(results, 1):
            # Truncate fields for single-line display
            author_short = (author[:15] + "...") if author and len(author) > 15 else (author or "")
            title_short = (title[:25] + "...") if title and len(title) > 25 else (title or "")
            gloss_short = (gloss[:15] + "...") if gloss and len(gloss) > 15 else (gloss or "")
            
            # Build display line
            display_parts = [p for p in [author_short, title_short, gloss_short] if p]
            display = " - ".join(display_parts)
            
            print(f"   {i:2d}. [{uid:4d}] {display}")
        
        # Get selection
        choice = input(f"\n   Select (1-{len(results)}), search again (s), or skip (Enter): ").strip().lower()
        
        if choice == 's':
            continue
        elif not choice:
            return None
        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(results):
                return results[idx][0]  # Return UID
            else:
                print(f"   ❌ Invalid selection")


def _get_specificity_selection(cursor):
    """
    Helper function to get Specificity value(s) from tokenized existing values.
    Allows multi-select from existing space-separated tokens.
    
    Args:
        cursor: Database cursor
    
    Returns:
        str: Space-separated specificity values, or None if skipped
    """
    # Get all existing specificity values
    cursor.execute("SELECT DISTINCT Specificity FROM definitions WHERE Specificity IS NOT NULL")
    all_values = cursor.fetchall()
    
    # Tokenize them (split by spaces)
    tokens = set()
    for (value,) in all_values:
        if value:
            # Split on spaces and add each token
            tokens.update(value.split())
    
    if not tokens:
        # No existing values, allow free entry
        print("\nSpecificity (no existing values, free entry):")
        return input("Specificity: ").strip() or None
    
    # Display tokens for selection
    tokens_list = sorted(tokens)
    
    print("\nSpecificity - Select from existing values (can select multiple):")
    for i, token in enumerate(tokens_list, 1):
        print(f"   {i:2d}. {token}")
    
    print("\n   Enter numbers separated by spaces (e.g., '1 3 5')")
    print("   Or press Enter to skip")
    
    selection = input("   Selection: ").strip()
    
    if not selection:
        return None
    
    # Parse selection
    selected_tokens = []
    for num in selection.split():
        if num.isdigit():
            idx = int(num) - 1
            if 0 <= idx < len(tokens_list):
                selected_tokens.append(tokens_list[idx])
            else:
                print(f"   ⚠️  Skipping invalid selection: {num}")
    
    if not selected_tokens:
        return None
    
    # Join with spaces
    return " ".join(selected_tokens)