#!/usr/bin/env python3
"""
Library of functions for querying Eurasia database
"""

import sqlite3, os
import pandas as pd
import re
from datetime import datetime
import pyperclip

"""
Setting up the database, confirming connection, and listing tables.
"""

#set home directory path
hdir = os.path.expanduser('~')

dh_path = '/Dropbox/Active_Directories/Digital_Humanities/'
inbox_path = os.path.join(hdir, 'Dropbox/Active_Directories/Inbox')

database_path = os.path.join(hdir, dh_path.strip('/'), 'database_eurasia_7.0.db')


# Check if database file exists
if not os.path.exists(database_path):
    raise FileNotFoundError(f"Database file not found at: {database_path}")

# Connect to the SQLite database
conn = sqlite3.connect(database_path)

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

try:
    # Your database operations will go here
    pass
finally:
    # Always close the connection when done
    cursor.close()
    conn.close()

"""Display Configuration Etc
"""

def _configure_display():
    """Configure pandas display options for better terminal viewing"""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None) 
    pd.set_option('display.max_colwidth', 80)
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.precision', 2)  # For any numeric data


"""
Database Information Functions"""

def database_info(table_name=None, show_columns=False):
    """
    Display database information.
    
    Args:
        table_name (str, optional): Specific table to examine. If None, shows all tables.
        show_columns (bool): If True, shows column details for the specified table(s).
    
    Examples:
        database_info()                    # List all tables with basic info
        database_info('lexicon')           # Show basic info for lexicon table
        database_info('lexicon', True)     # Show lexicon table with full column details
        database_info(show_columns=True)   # Show all tables with full column details
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    try:
        if table_name:
            # Show info for specific table
            _show_table_info(cursor, table_name, show_columns)
        else:
            # Show info for all tables
            cursor.execute("""
                SELECT name 
                FROM sqlite_master 
                WHERE type='table'
                ORDER BY name;
            """)
            tables = cursor.fetchall()
            
            print("📊 Database Tables Overview:")
            print("=" * 50)
            
            for table in tables:
                _show_table_info(cursor, table[0], show_columns)
                
    finally:
        cursor.close()
        conn.close()

def _show_table_info(cursor, table_name, show_columns=False):
    """Helper function to display information about a single table"""
    try:
        # Get basic table info
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = cursor.fetchall()
        num_columns = len(columns_info)

        cursor.execute(f"PRAGMA foreign_key_list({table_name});")
        foreign_keys = cursor.fetchall()
        foreign_keys_info = [fk[3] for fk in foreign_keys]

        print(f"📋 {table_name}: {num_columns} columns, FK: {foreign_keys_info}")
        
        if show_columns:
            print("   Columns:")
            for col in columns_info:
                col_name, col_type, not_null, default, pk = col[1], col[2], col[3], col[4], col[5]
                pk_indicator = " (PK)" if pk else ""
                print(f"     • {col_name} ({col_type}){pk_indicator}")
            print()
            
    except Exception as e:
        print(f"❌ Error examining table {table_name}: {e}")


def validate_search_config(table_name=None, verbose=True):
    """
    Validate TABLE_SEARCH_CONFIG against actual database schema.
    
    This function checks that all configured search_fields and display_fields
    actually exist in their respective tables. Useful for catching typos or
    outdated configs after schema changes.
    
    Args:
        table_name (str, optional): Specific table to validate. If None, validates all configured tables.
        verbose (bool): If True, prints detailed info about each field. If False, only prints errors.
    
    Returns:
        dict: Summary of validation results with counts of valid/invalid fields per table
    
    Examples:
        validate_search_config()                    # Check all configured tables
        validate_search_config('prosopography')     # Check specific table
        validate_search_config(verbose=False)       # Only show problems
    
    Use this function:
        - After modifying TABLE_SEARCH_CONFIG
        - After changing database schema (renaming/removing columns)
        - To debug search errors
        - Before deploying config changes
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    # Import TABLE_SEARCH_CONFIG from the module scope
    # (If this errors, TABLE_SEARCH_CONFIG hasn't been defined yet)
    global TABLE_SEARCH_CONFIG
    
    validation_results = {}
    tables_to_check = [table_name] if table_name else list(TABLE_SEARCH_CONFIG.keys())
    
    try:
        for table in tables_to_check:
            if table not in TABLE_SEARCH_CONFIG:
                print(f"❌ Table '{table}' not found in TABLE_SEARCH_CONFIG")
                continue
            
            config = TABLE_SEARCH_CONFIG[table]
            
            # Get actual columns from database
            cursor.execute(f"PRAGMA table_info({table});")
            table_columns = {col[1] for col in cursor.fetchall()}  # Use set for fast lookup
            
            if verbose:
                print(f"\n{'='*70}")
                print(f"📋 Validating: {table}")
                print(f"{'='*70}")
                print(f"   Table has {len(table_columns)} columns: {sorted(table_columns)}\n")
            
            # Validate search_fields
            search_fields = config.get('search_fields', [])
            valid_search = [f for f in search_fields if f in table_columns]
            invalid_search = [f for f in search_fields if f not in table_columns]
            
            if verbose or invalid_search:
                print(f"   🔍 Search Fields ({len(search_fields)} configured):")
                if valid_search:
                    print(f"      ✅ Valid ({len(valid_search)}): {valid_search}")
                if invalid_search:
                    print(f"      ❌ INVALID ({len(invalid_search)}): {invalid_search}")
            
            # Validate display_fields
            display_fields = config.get('display_fields', [])
            valid_display = [f for f in display_fields if f in table_columns]
            invalid_display = [f for f in display_fields if f not in table_columns]
            
            if verbose or invalid_display:
                print(f"   📊 Display Fields ({len(display_fields)} configured):")
                if valid_display:
                    print(f"      ✅ Valid ({len(valid_display)}): {valid_display}")
                if invalid_display:
                    print(f"      ❌ INVALID ({len(invalid_display)}): {invalid_display}")
            
            # Validate foreign_keys (check that the FK column exists in this table)
            foreign_keys = config.get('foreign_keys', {})
            valid_fks = []
            invalid_fks = []
            
            for fk_column, fk_config in foreign_keys.items():
                if fk_column in table_columns:
                    # Also check that referenced table and field exist
                    ref_table = fk_config['table']
                    ref_field = fk_config['display_field']
                    
                    cursor.execute(f"PRAGMA table_info({ref_table});")
                    ref_columns = {col[1] for col in cursor.fetchall()}
                    
                    if ref_field in ref_columns:
                        valid_fks.append(f"{fk_column} → {ref_table}.{ref_field}")
                    else:
                        invalid_fks.append(f"{fk_column} → {ref_table}.{ref_field} (field doesn't exist)")
                else:
                    invalid_fks.append(f"{fk_column} (column doesn't exist)")
            
            if verbose or invalid_fks:
                print(f"   🔗 Foreign Keys ({len(foreign_keys)} configured):")
                if valid_fks:
                    print(f"      ✅ Valid ({len(valid_fks)}):")
                    for fk in valid_fks:
                        print(f"         • {fk}")
                if invalid_fks:
                    print(f"      ❌ INVALID ({len(invalid_fks)}):")
                    for fk in invalid_fks:
                        print(f"         • {fk}")
            
            # Store results
            validation_results[table] = {
                'valid_search': len(valid_search),
                'invalid_search': len(invalid_search),
                'valid_display': len(valid_display),
                'invalid_display': len(invalid_display),
                'valid_fks': len(valid_fks),
                'invalid_fks': len(invalid_fks),
                'has_errors': bool(invalid_search or invalid_display or invalid_fks)
            }
        
        # Print summary
        print(f"\n{'='*70}")
        print("📊 VALIDATION SUMMARY")
        print(f"{'='*70}")
        
        tables_with_errors = [t for t, r in validation_results.items() if r['has_errors']]
        tables_valid = [t for t, r in validation_results.items() if not r['has_errors']]
        
        if tables_valid:
            print(f"✅ Valid tables ({len(tables_valid)}):")
            for t in tables_valid:
                r = validation_results[t]
                print(f"   • {t}: {r['valid_search']} search, {r['valid_display']} display, {r['valid_fks']} FKs")
        
        if tables_with_errors:
            print(f"\n❌ Tables with errors ({len(tables_with_errors)}):")
            for t in tables_with_errors:
                r = validation_results[t]
                errors = []
                if r['invalid_search']:
                    errors.append(f"{r['invalid_search']} invalid search")
                if r['invalid_display']:
                    errors.append(f"{r['invalid_display']} invalid display")
                if r['invalid_fks']:
                    errors.append(f"{r['invalid_fks']} invalid FKs")
                print(f"   • {t}: {', '.join(errors)}")
        else:
            print(f"\n🎉 All configured tables are valid!")
        
        return validation_results
        
    except Exception as e:
        print(f"❌ Validation error: {e}")
        import traceback
        traceback.print_exc()
        return {}
    finally:
        cursor.close()
        conn.close()


def _show_table_info(cursor, table_name, show_columns=False):
    """Helper function to display information about a single table"""
    try:
        # Get basic table info
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = cursor.fetchall()
        num_columns = len(columns_info)

        cursor.execute(f"PRAGMA foreign_key_list({table_name});")
        foreign_keys = cursor.fetchall()
        foreign_keys_info = [fk[3] for fk in foreign_keys]

        print(f"📋 {table_name}: {num_columns} columns, FK: {foreign_keys_info}")
        
        if show_columns:
            print("   Columns:")
            for col in columns_info:
                col_name, col_type, not_null, default, pk = col[1], col[2], col[3], col[4], col[5]
                pk_indicator = " (PK)" if pk else ""
                print(f"     • {col_name} ({col_type}){pk_indicator}")
            print()
            
    except Exception as e:
        print(f"❌ Error examining table {table_name}: {e}")

"""
Database Query Functions
"""

def get_unique_values(table_name, column_name):
    """
    Retrieve a list of all unique values in the specified column of a table.
    """
    # Establish a connection to the database using the database_path
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    # Query to select distinct values from the specified column
    query = f"SELECT DISTINCT {column_name} FROM {table_name};"
    cursor.execute(query)
    
    # Fetch all unique values
    unique_values = [row[0] for row in cursor.fetchall()]
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    
    return unique_values


"""
Series of functions that allow regex querying of lexicon-related tables in the database.
"""



# Function to enable regex in SQLite
def _regex_search(pattern, string):
    # Check if the string is valid
    if not isinstance(string, str):
        return False
    try:
        return re.search(pattern, string) is not None
    except Exception as e:
        print(f"Regex error: {e}")
        return False

# Register the regex function with SQLite
def _register_regex(conn):
    conn.create_function("REGEXP", 2, _regex_search)

def word_search(search_term, filter=None, max_results=None, save_report=False):
    """
    Search for terms in the lexicon table using regex and return results with definitions and related terms.
    
    Args:
        search_term (str): Regex pattern to search for
        filter (str, optional): Filter results by Scope, Etymology, or Tags columns (regex match)
        max_results (int, optional): Maximum number of results to display (default: None = unlimited)
        save_report (bool): If True, saves results as markdown report to Inbox
    """
    conn = sqlite3.connect(database_path)
    _register_regex(conn)
    cursor = conn.cursor()

    print(f"🔍 Searching for: '{search_term}'" + (f" (showing up to {max_results} results per section)" if max_results else ""))
    if filter:
        print(f"   Filter: '{filter}' (on Scope, Etymology, or Tags)")
    print("=" * 80)

    try:
        # 1. First get count of matching lexicon entries
        if filter:
            count_query = """
                SELECT COUNT(DISTINCT l.UID)
                FROM lexicon l
                WHERE (l.Term REGEXP ? OR l.Translation REGEXP ? OR l.Emic_Term REGEXP ? 
                   OR l.Colonial_Term REGEXP ? OR l.Transliteration REGEXP ?)
                   AND (l.Scope REGEXP ? OR l.Etymology REGEXP ? OR l.Tags REGEXP ?);
            """
            cursor.execute(count_query, (search_term, search_term, search_term, search_term, search_term, 
                                         filter, filter, filter))
        else:
            count_query = """
                SELECT COUNT(DISTINCT l.UID)
                FROM lexicon l
                WHERE l.Term REGEXP ? OR l.Translation REGEXP ? OR l.Emic_Term REGEXP ? 
                   OR l.Colonial_Term REGEXP ? OR l.Transliteration REGEXP ?;
            """
            cursor.execute(count_query, (search_term, search_term, search_term, search_term, search_term))
        
        lexicon_total = cursor.fetchone()[0]

        # 2. Get matching lexicon entries with their definitions
        # First, get the limited set of UIDs (or all if max_results is None)
        if filter:
            uid_query = """
                SELECT DISTINCT l.UID
                FROM lexicon l
                WHERE (l.Term REGEXP ? OR l.Translation REGEXP ? OR l.Emic_Term REGEXP ? 
                   OR l.Colonial_Term REGEXP ? OR l.Transliteration REGEXP ?)
                   AND (l.Scope REGEXP ? OR l.Etymology REGEXP ? OR l.Tags REGEXP ?)
                ORDER BY LENGTH(COALESCE(l.Term, l.Emic_Term))
            """
            params = (search_term, search_term, search_term, search_term, search_term, 
                     filter, filter, filter)
            if max_results:
                uid_query += " LIMIT ?;"
                params = params + (max_results,)
            cursor.execute(uid_query, params)
        else:
            uid_query = """
                SELECT DISTINCT l.UID
                FROM lexicon l
                WHERE l.Term REGEXP ? OR l.Translation REGEXP ? OR l.Emic_Term REGEXP ? 
                   OR l.Colonial_Term REGEXP ? OR l.Transliteration REGEXP ?
                ORDER BY LENGTH(COALESCE(l.Term, l.Emic_Term))
            """
            params = (search_term, search_term, search_term, search_term, search_term)
            if max_results:
                uid_query += " LIMIT ?;"
                params = params + (max_results,)
            cursor.execute(uid_query, params)
        
        limited_uids = [row[0] for row in cursor.fetchall()]
        
        if not limited_uids:
            lexicon_results = []
            matched_uids = []
        else:
            # Now get all data for these UIDs including all their definitions
            placeholders = ','.join(['?' for _ in limited_uids])
            query = f"""
                SELECT 
                    l.UID,
                    l.Term,
                    l.Translation,
                    l.Emic_Term,
                    l.Colonial_Term,
                    l.Transliteration,
                    l.Etymology,
                    l.Scope,
                    l.Tags,
                    d.Definition,
                    d.Type
                FROM lexicon l
                LEFT JOIN definitions d ON l.UID = d.Lexicon_ID
                WHERE l.UID IN ({placeholders})
                ORDER BY LENGTH(COALESCE(l.Term, l.Emic_Term));
            """
            cursor.execute(query, limited_uids)
            lexicon_results = cursor.fetchall()
            matched_uids = limited_uids

        print(f"📚 LEXICON ENTRIES (displaying {len(set([r[0] for r in lexicon_results]))} out of {lexicon_total} matches)")
        print("-" * 40)
        
        if lexicon_results:
            # Group by UID to handle multiple definitions
            entry_dict = {}
            for uid, term, translation, emic, colonial, translit, etymology, scope, tags, definition, def_type in lexicon_results:
                if uid not in entry_dict:
                    entry_dict[uid] = {
                        'term': term,
                        'translation': translation,
                        'emic': emic,
                        'colonial': colonial,
                        'translit': translit,
                        'etymology': etymology,
                        'scope': scope,
                        'tags': tags,
                        'definitions': []
                    }
                if definition:
                    entry_dict[uid]['definitions'].append((def_type, definition))
            
            for i, (uid, data) in enumerate(entry_dict.items(), 1):
                # Display the main term
                main_display = data['term'] or data['emic'] or data['translit']
                print(f"{i}. {main_display}")
                
                if data['translation']:
                    print(f"   🔤 Translation: {data['translation']}")
                if data['emic']:
                    print(f"   🔤 Emic Term: {data['emic']}")
                if data['colonial']:
                    print(f"   🔤 Colonial Term: {data['colonial']}")
                if data['translit']:
                    print(f"   🔤 Transliteration: {data['translit']}")
                if data['etymology']:
                    print(f"   🌱 Etymology: {data['etymology']}")
                if data['scope']:
                    print(f"   📍 Scope: {data['scope']}")
                if data['tags']:
                    print(f"   🏷️ Tags: {data['tags']}")
                
                # Display definitions
                if data['definitions']:
                    for def_type, definition in data['definitions']:
                        if def_type:
                            print(f"   📖 {def_type}: {definition}")
                        else:
                            print(f"   📖 {definition}")
                print()
        else:
            print("   No matches found\n")

        # 3. Get related terms for matched entries
        if matched_uids:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM related_terms rt
                WHERE rt.Parent_ID IN ({','.join(['?' for _ in matched_uids])});
            """, matched_uids)
            related_total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT 
                    pl.Term as parent_term,
                    rt.Type,
                    cl.Term as child_term,
                    cl.Translation as child_translation
                FROM related_terms rt
                JOIN lexicon pl ON rt.Parent_ID = pl.UID
                JOIN lexicon cl ON rt.Child_ID = cl.UID
                WHERE rt.Parent_ID IN ({','.join(['?' for _ in matched_uids])})
            """ + (" LIMIT ?" if max_results else ""),
            matched_uids + ([max_results] if max_results else []))

            related_results = cursor.fetchall()
            
            print(f"🔗 RELATED TERMS (displaying {len(related_results)} out of {related_total} matches)")
            print("-" * 40)
            
            if related_results:
                for i, (parent, rel_type, child, child_trans) in enumerate(related_results, 1):
                    print(f"{i}. {parent} → {child}")
                    if rel_type:
                        print(f"   📝 Type: {rel_type}")
                    if child_trans:
                        print(f"   🔤 Translation: {child_trans}")
                    print()
            else:
                print("   No related terms found\n")

        # Summary
        print("=" * 80)
        print(f"📊 SUMMARY: {len(set([r[0] for r in lexicon_results]))} lexicon entries, {len(related_results) if matched_uids else 0} related terms")

    except Exception as e:
        print(f"❌ Search error: {e}")
    finally:
        cursor.close()
        conn.close()


def location_search(search_term, max_results=None, save_report=False):
    """
    Search for locations in the gazetteer and show related attributes and hierarchies.
    
    Args:
        search_term (str): Regex pattern to search for
        max_results (int, optional): Maximum number of results to display per section (default: None = unlimited)
        save_report (bool): If True, saves results as markdown report to Inbox
    """
    conn = sqlite3.connect(database_path)
    _register_regex(conn)
    cursor = conn.cursor()

    print(f"🔍 Searching for: '{search_term}'" + (f" (showing up to {max_results} results per section)" if max_results else ""))
    print("=" * 80)

    try:
        # 1. Search gazetteer and get total count
        cursor.execute("""
            SELECT COUNT(*)
            FROM gazetteer
            WHERE Nickname REGEXP ? OR Location_Name_Arabic REGEXP ? 
               OR Location_Name_Colonial REGEXP ? OR Location_Name_Latin REGEXP ?;
        """, (search_term, search_term, search_term, search_term))
        gazetteer_total = cursor.fetchone()[0]

        cursor.execute("""
            SELECT UID, Nickname, Location_Name_Arabic, Location_Name_Colonial, Location_Name_Latin
            FROM gazetteer
            WHERE Nickname REGEXP ? OR Location_Name_Arabic REGEXP ? 
               OR Location_Name_Colonial REGEXP ? OR Location_Name_Latin REGEXP ?
            ORDER BY LENGTH(COALESCE(Nickname, Location_Name_Latin))
        """ + (" LIMIT ?" if max_results else ""), 
        (search_term, search_term, search_term, search_term) + ((max_results,) if max_results else ()))

        gazetteer_results = cursor.fetchall()
        matched_uids = [row[0] for row in gazetteer_results]

        print(f"📍 GAZETTEER ENTRIES (displaying {len(gazetteer_results)} out of {gazetteer_total} matches)")
        print("-" * 40)
        
        if gazetteer_results:
            for i, (uid, nickname, arabic, colonial, latin) in enumerate(gazetteer_results, 1):
                print(f"{i}. {nickname}")
                if arabic:
                    print(f"   🔤 Arabic: {arabic}")
                if colonial:
                    print(f"   🔤 Colonial: {colonial}")
                if latin:
                    print(f"   🔤 Latin: {latin}")
                print()
        else:
            print("   No matches found\n")

        # 2. Get location attributes for matched locations
        if matched_uids:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM location_attributes
                WHERE Location_ID IN ({','.join(['?' for _ in matched_uids])});
            """, matched_uids)
            attributes_total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT 
                    g.Nickname,
                    la.Type,
                    la.Description,
                    la.Date_Start,
                    la.Date_End
                FROM location_attributes la
                JOIN gazetteer g ON la.Location_ID = g.UID
                WHERE la.Location_ID IN ({','.join(['?' for _ in matched_uids])})
            """ + (" LIMIT ?" if max_results else ""), 
            matched_uids + ([max_results] if max_results else []))

            attributes_results = cursor.fetchall()
            
            print(f"📋 LOCATION ATTRIBUTES (displaying {len(attributes_results)} out of {attributes_total} matches)")
            print("-" * 40)
            
            if attributes_results:
                for i, (nickname, loc_type, description, date_start, date_end) in enumerate(attributes_results, 1):
                    print(f"{i}. {nickname}")
                    if loc_type:
                        print(f"   📝 Type: {loc_type}")
                    if description:
                        print(f"   📖 {description}")
                    if date_start or date_end:
                        date_range = f"{date_start or '?'} - {date_end or '?'}"
                        print(f"   📅 Period: {date_range}")
                    print()
            else:
                print("   No attributes found\n")

            # 3. Get location hierarchies
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM location_hierarchies
                WHERE Child_ID IN ({','.join(['?' for _ in matched_uids])})
                   OR Parent_ID IN ({','.join(['?' for _ in matched_uids])});
            """, matched_uids + matched_uids)
            hierarchies_total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT 
                    gc.Nickname as child_name,
                    lh.Relationship,
                    gp.Nickname as parent_name
                FROM location_hierarchies lh
                JOIN gazetteer gc ON lh.Child_ID = gc.UID
                JOIN gazetteer gp ON lh.Parent_ID = gp.UID
                WHERE lh.Child_ID IN ({','.join(['?' for _ in matched_uids])})
                   OR lh.Parent_ID IN ({','.join(['?' for _ in matched_uids])})
            """ + (" LIMIT ?" if max_results else ""),
            matched_uids + matched_uids + ([max_results] if max_results else []))

            hierarchies_results = cursor.fetchall()
            
            print(f"🏛️ LOCATION HIERARCHIES (displaying {len(hierarchies_results)} out of {hierarchies_total} matches)")
            print("-" * 40)
            
            if hierarchies_results:
                for i, (child, relationship, parent) in enumerate(hierarchies_results, 1):
                    print(f"{i}. {child} → {parent}")
                    if relationship:
                        print(f"   📝 Relationship: {relationship}")
                    print()
            else:
                print("   No hierarchies found\n")

        # Summary
        print("=" * 80)
        locations_count = len(gazetteer_results)
        attributes_count = len(attributes_results) if matched_uids else 0
        hierarchies_count = len(hierarchies_results) if matched_uids else 0
        print(f"📊 SUMMARY: {locations_count} locations, {attributes_count} attributes, {hierarchies_count} hierarchies")

    except Exception as e:
        print(f"❌ Search error: {e}")
    finally:
        cursor.close()
        conn.close()


def _auto_detect_search_config(cursor, table_name):
    """
    Automatically detect searchable fields and important display fields.
    Used as fallback when table not in TABLE_SEARCH_CONFIG.
    
    Args:
        cursor: Database cursor
        table_name: Name of table to analyze
    
    Returns:
        dict: Configuration dictionary with search_fields, display_fields, foreign_keys, emoji
    """
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    
    search_fields = []
    display_fields = ['UID']  # Always include UID first
    
    for col in columns:
        col_name = col[1]
        col_type = col[2]
        
        # Skip UID in loop since we already added it
        if col_name == 'UID':
            continue
        
        # Auto-include text fields for searching
        if col_type in ('TEXT', 'VARCHAR', 'CHAR'):
            # Exclude IDs but include everything else by default
            if '_ID' not in col_name and col_name != 'UID':
                search_fields.append(col_name)
        
        # Auto-include key display fields (but not Notes/Description unless no other fields)
        if any(x in col_name.lower() for x in ['name', 'title', 'term', 'date', 'nickname', 'type', 'year']):
            if col_name not in display_fields:
                display_fields.append(col_name)
    
    # If no search fields found, use first 3 text columns (excluding UID and IDs)
    if not search_fields:
        search_fields = [col[1] for col in columns 
                        if col[2] in ('TEXT', 'VARCHAR', 'CHAR') 
                        and col[1] != 'UID' 
                        and '_ID' not in col[1]][:3]
    
    # If display fields only has UID, add first few non-ID columns
    if len(display_fields) == 1:
        for col in columns[:5]:
            if col[1] != 'UID' and '_ID' not in col[1] and col[1] not in display_fields:
                display_fields.append(col[1])
    
    return {
        'search_fields': search_fields,
        'display_fields': display_fields,
        'foreign_keys': {},
        'related_tables': [],
        'emoji': '🔍'
    }


def _get_notes_fields(search_fields):
    """
    Identify Notes/Description fields in search_fields list.
    
    Args:
        search_fields: List of field names
    
    Returns:
        list: Field names that are Notes/Description type fields
    """
    notes_keywords = ['Notes', 'Description', 'Comments']
    return [f for f in search_fields if any(keyword in f for keyword in notes_keywords)]


def _display_related_records(cursor, table, uid, related_tables_config):
    """
    Display related records from junction tables.
    
    Args:
        cursor: Database cursor
        table: Current table name
        uid: UID of current record
        related_tables_config: List of dicts with relationship info
            Each dict should contain:
                - junction_table: Name of the junction/linking table
                - junction_fk: FK in junction table pointing to current table
                - target_fk: FK in junction table pointing to related table
                - target_table: Name of the related target table
                - target_display: Field to display from target table
                - label: Human-readable label for the relationship
    
    Example:
        Related to a person through relationships table:
        {
            'junction_table': 'relationships',
            'junction_fk': 'Parent',
            'target_fk': 'Child',
            'target_table': 'prosopography',
            'target_display': 'Name_Latin',
            'label': 'Children'
        }
    """
    for rel_config in related_tables_config:
        junction_table = rel_config['junction_table']
        junction_fk = rel_config['junction_fk']
        target_fk = rel_config['target_fk']
        target_table = rel_config['target_table']
        target_display = rel_config['target_display']
        label = rel_config['label']
        
        # Build query to find related records through junction table
        query = f"""
            SELECT t.{target_display}
            FROM {junction_table} j
            JOIN {target_table} t ON j.{target_fk} = t.UID
            WHERE j.{junction_fk} = ?
            LIMIT 10;
        """
        
        try:
            cursor.execute(query, (uid,))
            related = cursor.fetchall()
            
            if related:
                related_names = [r[0] for r in related if r[0]]  # Filter out None values
                if related_names:
                    # Truncate if too many
                    if len(related_names) > 5:
                        display = ', '.join(related_names[:5]) + f' (+{len(related_names) - 5} more)'
                    else:
                        display = ', '.join(related_names)
                    print(f"   🔗 {label}: {display}")
        except Exception as e:
            # Silently skip if there's an error with this relationship
            pass


def _display_definitions(cursor, uid, definitions_table, fk_column):
    """
    Display definitions for lexicon or social_roles entries.
    
    This is a special case because definitions are stored in a separate table
    but we want to display the definition text itself, not just join to another table.
    
    Args:
        cursor: Database cursor
        uid: UID of the lexicon or social_roles entry
        definitions_table: Name of the definitions table (usually 'definitions')
        fk_column: Foreign key column name ('Lexicon_ID' or 'Social_Role_ID')
    
    Example SQL generated:
        SELECT Definition, Type 
        FROM definitions 
        WHERE Lexicon_ID = 123;
    """
    query = f"""
        SELECT `Definition`, `Type`
        FROM `{definitions_table}`
        WHERE `{fk_column}` = ?
        LIMIT 5;
    """
    
    try:
        cursor.execute(query, (uid,))
        definitions = cursor.fetchall()
        
        if definitions:
            print(f"   📖 Definitions:")
            for definition, def_type in definitions:
                if definition:
                    # Truncate long definitions
                    if len(definition) > 200:
                        definition = definition[:200] + "..."
                    if def_type:
                        print(f"      • [{def_type}] {definition}")
                    else:
                        print(f"      • {definition}")
    except Exception as e:
        # Silently skip if there's an error
        pass


def _display_location_attributes(cursor, location_uid):
    """
    Display location attributes as Type: Value pairs.
    
    For a location in the gazetteer, this shows various attributes like:
    - fortress: Khujand Citadel
    - river: Syr Darya
    - climate: continental
    
    Args:
        cursor: Database cursor
        location_uid: UID of the location in gazetteer table
    
    Example SQL generated:
        SELECT Type, Value, Start_Date_Greg, End_Date_Greg
        FROM location_attributes
        WHERE Location_ID = 74;
    """
    query = """
        SELECT `Type`, `Value`, `Start_Date_Greg`, `End_Date_Greg`
        FROM `location_attributes`
        WHERE `Location_ID` = ?
        ORDER BY `Type`
        LIMIT 10;
    """
    
    try:
        cursor.execute(query, (location_uid,))
        attributes = cursor.fetchall()
        
        if attributes:
            print(f"   🏛️ Attributes:")
            for attr_type, value, start_date, end_date in attributes:
                if attr_type and value:
                    # Build date range if available
                    date_info = ""
                    if start_date or end_date:
                        dates = f"{start_date or '?'} - {end_date or '?'}"
                        date_info = f" ({dates})"
                    
                    print(f"      • {attr_type}: {value}{date_info}")
    except Exception as e:
        # Silently skip if there's an error
        pass


def _biblio_serials(search_term, repository_filter=None, max_results=None):
    """
    Internal function: Search bibliography and return list of matching UIDs.
    
    Args:
        search_term (str or tuple): Regex pattern(s) to search in Author, Title, and Gloss columns
        repository_filter (str or tuple, optional): Filter pattern(s) for repository columns
        max_results (int, optional): Maximum number of UIDs to return
    
    Returns:
        list: List of matching UIDs (integers)
    
    Example:
        uids = _biblio_serials('Bukhara')  # Returns [1, 5, 23, ...]
    """
    conn = sqlite3.connect(database_path)
    _register_regex(conn)
    cursor = conn.cursor()

    # Convert single strings to tuples for uniform handling
    search_terms = (search_term,) if isinstance(search_term, str) else search_term
    repo_filters = (repository_filter,) if isinstance(repository_filter, str) else repository_filter if repository_filter else None

    try:
        # Build search_term WHERE clause - OR logic across all patterns and columns
        search_conditions = []
        search_params = []
        for term in search_terms:
            search_conditions.append("(b.Author REGEXP ? OR b.Title REGEXP ? OR b.Gloss REGEXP ?)")
            search_params.extend([term, term, term])
        
        search_where = " OR ".join(search_conditions)
        
        # Build repository_filter WHERE clause - AND logic across all patterns
        repo_where = ""
        repo_params = []
        if repo_filters:
            repo_conditions = []
            for filter_term in repo_filters:
                repo_conditions.append(
                    "(r.Acronym REGEXP ? OR r.Name_Foreign REGEXP ? OR r.Name_English REGEXP ? "
                    "OR b.Language REGEXP ? OR b.Status REGEXP ? OR b.Tags REGEXP ?)"
                )
                repo_params.extend([filter_term] * 6)
            repo_where = " AND " + " AND ".join(repo_conditions)
        
        # Query to get just UIDs
        query = f"""
            SELECT b.UID
            FROM bibliography b
            LEFT JOIN repositories r ON b.Repository_ID = r.UID
            WHERE ({search_where}){repo_where}
        """
        
        params = search_params + repo_params
        
        # Get UIDs with optional limit
        cursor.execute(query + (" LIMIT ?" if max_results else ""), 
                      params + ([max_results] if max_results else []))
        
        # Extract UIDs from results and return as list
        uids = [row[0] for row in cursor.fetchall()]
        return uids

    except Exception as e:
        print(f"❌ Search error: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def bib_search(search_term, repository_filter=None, max_results=None, save_report=False):
    """
    Search for bibliography entries and show related sources and references.
    
    Args:
        search_term (str or tuple): Regex pattern(s) to search in Author, Title, and Gloss columns.
            - str: Single pattern (OR across columns)
            - tuple: Multiple patterns (OR across all - matches ANY pattern in ANY column)
        repository_filter (str or tuple, optional): Filter pattern(s) for Acronym, Name_Foreign, 
            Name_English, Language, Status, and Tags columns.
            - str: Single pattern (must match ANY of the 6 columns)
            - tuple: Multiple patterns (must match ALL patterns - each in ANY of the 6 columns)
            - None: No repository filtering
        max_results (int, optional): Maximum results per section (default: None = unlimited)
        save_report (bool): If True, saves markdown report to Inbox
    
    Returns:
        None (prints formatted results to console)
    
    Examples:
        bib_search('Bukhara')                           # Single term in Author/Title/Gloss
        bib_search(('تاریخ', 'history'))                # Match either term
        bib_search('Samarqand', 'edited')               # Filter to edited works
        bib_search('trade', ('Farsi', 'manuscript'))    # Must be Farsi AND manuscript
    """
    # Convert single strings to tuples for uniform handling
    search_terms = (search_term,) if isinstance(search_term, str) else search_term
    repo_filters = (repository_filter,) if isinstance(repository_filter, str) else repository_filter if repository_filter else None

    # Display search info
    print(f"🔍 Searching for: {search_terms}" + (f" (showing up to {max_results} results per section)" if max_results else ""))
    if repo_filters:
        print(f"   Repository filter (ALL must match): {repo_filters}")
    print("=" * 80)

    # Get matching UIDs using the internal function
    matched_uids = _biblio_serials(search_term, repository_filter, max_results)
    
    if not matched_uids:
        print("📚 BIBLIOGRAPHY ENTRIES (displaying 0 matches)")
        print("-" * 40)
        print("   No matches found\n")
        print("=" * 80)
        print(f"📊 SUMMARY: 0 bibliography entries, 0 related sources")
        return

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    try:
        # Get full details for these UIDs
        placeholders = ','.join(['?' for _ in matched_uids])
        
        # Get total count (for display purposes)
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM bibliography b
            WHERE b.UID IN ({placeholders})
        """, matched_uids)
        bibliography_total = cursor.fetchone()[0]

        # Get full bibliography details
        cursor.execute(f"""
            SELECT b.UID, b.Author, b.Title, b.Gloss, b.Date_Pub_Greg, b.Date_Pub_Hij, 
                   r.Acronym, r.Name_English, b.Catalog_No, b.Language, b.Status, b.Tags
            FROM bibliography b
            LEFT JOIN repositories r ON b.Repository_ID = r.UID
            WHERE b.UID IN ({placeholders})
        """, matched_uids)
        
        bibliography_results = cursor.fetchall()

        print(f"📚 BIBLIOGRAPHY ENTRIES (displaying {len(bibliography_results)} out of {bibliography_total} matches)")
        print("-" * 40)
        
        for i, (uid, author, title, gloss, date_greg, date_hij, acronym, repo_name, 
               catalog, language, status, tags) in enumerate(bibliography_results, 1):
            print(f"{i}. {author} - {title}")
            if gloss:
                print(f"   📝 Gloss: {gloss}")
            if uid:
                print(f"   🔑 UID: {uid}")
            if acronym:
                print(f"   🏛️ Repository: {acronym}" + (f" ({repo_name})" if repo_name else ""))
            if catalog:
                print(f"   📋 Catalog: {catalog}")
            if date_greg:
                print(f"   📅 Date (Gregorian): {date_greg}")
            if date_hij:
                print(f"   📅 Date (Hijri): {date_hij}")
            # Show filter columns when repository filtering is active
            if repo_filters:
                if language:
                    print(f"   🌐 Language: {language}")
                if status:
                    print(f"   📊 Status: {status}")
                if tags:
                    # Clean up tags: split on whitespace, remove empty strings, join with commas
                    clean_tags = ', '.join(filter(None, tags.split()))
                    print(f"   🏷️  Tags: {clean_tags}")
            print()

        # Get related sources
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM related_sources
            WHERE Referencing_Source_ID IN ({placeholders})
               OR Referenced_Source_ID IN ({placeholders});
        """, matched_uids + matched_uids)
        related_total = cursor.fetchone()[0]

        cursor.execute(f"""
            SELECT 
                b1.Author as ref_author,
                b1.Title as ref_title,
                rs.Type,
                b2.Author as refd_author,
                b2.Title as refd_title
            FROM related_sources rs
            JOIN bibliography b1 ON rs.Referencing_Source_ID = b1.UID
            JOIN bibliography b2 ON rs.Referenced_Source_ID = b2.UID
            WHERE rs.Referencing_Source_ID IN ({placeholders})
               OR rs.Referenced_Source_ID IN ({placeholders})
        """ + (" LIMIT ?" if max_results else ""),
        matched_uids + matched_uids + ([max_results] if max_results else []))

        related_sources = cursor.fetchall()
        
        print(f"🔗 RELATED SOURCES (displaying {len(related_sources)} out of {related_total} matches)")
        print("-" * 40)
        
        if related_sources:
            for i, (ref_auth, ref_title, rel_type, refd_auth, refd_title) in enumerate(related_sources, 1):
                print(f"{i}. {ref_auth}: {ref_title}")
                print(f"   → {refd_auth}: {refd_title}")
                if rel_type:
                    print(f"   📝 Type: {rel_type}")
                print()
        else:
            print("   No related sources found\n")

        # Summary
        print("=" * 80)
        entries_count = len(bibliography_results)
        related_count = len(related_sources)
        print(f"📊 SUMMARY: {entries_count} bibliography entries, {related_count} related sources")

    except Exception as e:
        print(f"❌ Display error: {e}")
    finally:
        cursor.close()
        conn.close()


"""
General Search Function with Table Configuration
"""

TABLE_SEARCH_CONFIG = {
    'bibliography': {
        'search_fields': ['Author', 'Title'],
        'display_fields': ['UID', 'Author', 'Title', 'Date_Pub_Greg', 'Date_Pub_Hij', 'Language', 'Catalog_No'],
        'foreign_keys': {
            'Repository_ID': {
                'table': 'repositories',
                'display_field': 'Acronym',
                'label': 'Repository'
            }
        },
        'related_tables': [
            {
                'junction_table': 'related_sources',
                'junction_fk': 'Referencing_Source_ID',
                'target_fk': 'Referenced_Source_ID',
                'target_table': 'bibliography',
                'target_display': 'Title',
                'label': 'Related Sources'
            }
        ],
        'emoji': '📚'
    },
    'prosopography': {
        'search_fields': ['Full_Name_Arabic', 'Full_Name_Latin', 'Nickname_Latin'],
        'display_fields': ['UID', 'Full_Name_Arabic', 'Full_Name_Latin', 'Nickname_Latin', 
                          'Birthdate_Greg', 'Deathdate_Greg', 'Social_Role'],
        'foreign_keys': {},
        'related_tables': [
            {
                'junction_table': 'relationships',
                'junction_fk': 'Parent',
                'target_fk': 'Child',
                'target_table': 'prosopography',
                'target_display': 'Full_Name_Latin',
                'label': 'Related To (Child)'
            },
            {
                'junction_table': 'relationships',
                'junction_fk': 'Child',
                'target_fk': 'Parent',
                'target_table': 'prosopography',
                'target_display': 'Full_Name_Latin',
                'label': 'Related To (Parent)'
            },
            {
                'junction_table': 'individual_social_roles',
                'junction_fk': 'Individual_ID',
                'target_fk': 'Social_Role_ID',
                'target_table': 'social_roles',
                'target_display': 'Role_Latin',
                'label': 'Social Roles'
            },
            {
                'junction_table': 'references_to_individuals',
                'junction_fk': 'Individual_ID',
                'target_fk': 'Source_ID',
                'target_table': 'bibliography',
                'target_display': 'Title',
                'label': 'Mentioned In'
            }
        ],
        'emoji': '👤'
    },
    'gazetteer': {
        'search_fields': ['Nickname', 'Location_Name_Arabic', 'Location_Name_Colonial', 'Location_Name_Latin'],
        'display_fields': ['UID', 'Nickname', 'Location_Name_Arabic', 'Location_Name_Colonial', 
                          'Location_Name_Latin', 'Type'],
        'foreign_keys': {},
        'related_tables': [
            {
                'junction_table': 'references_to_locations',
                'junction_fk': 'Location_ID',
                'target_fk': 'Source_ID',
                'target_table': 'bibliography',
                'target_display': 'Title',
                'label': 'Mentioned In'
            },
            {
                'junction_table': 'location_hierarchies',
                'junction_fk': 'Child_ID',
                'target_fk': 'Parent_ID',
                'target_table': 'gazetteer',
                'target_display': 'Nickname',
                'label': 'Part Of (Parent)'
            },
            {
                'junction_table': 'location_hierarchies',
                'junction_fk': 'Parent_ID',
                'target_fk': 'Child_ID',
                'target_table': 'gazetteer',
                'target_display': 'Nickname',
                'label': 'Contains (Children)'
            }
        ],
        'location_attributes_table': 'location_attributes',  # Special case: show Type: Value pairs
        'emoji': '📍'
    },
    'lexicon': {
        'search_fields': ['Term', 'Translation', 'Emic_Term', 'Colonial_Term', 'Transliteration'],
        'display_fields': ['UID', 'Term', 'Translation', 'Emic_Term', 'Colonial_Term', 'Transliteration', 
                          'Etymology', 'Scope', 'Tags'],
        'foreign_keys': {},
        'related_tables': [
            {
                'junction_table': 'related_terms',
                'junction_fk': 'Parent_ID',
                'target_fk': 'Child_ID',
                'target_table': 'lexicon',
                'target_display': 'Term',
                'label': 'Related Terms'
            }
        ],
        'definitions_table': 'definitions',  # Special case: get definitions directly
        'definitions_fk': 'Lexicon_ID',
        'emoji': '📖'
    },
    'social_roles': {
        'search_fields': ['Role_Emic', 'Role_Latin', 'Role_Translation'],
        'display_fields': ['UID', 'Role_Emic', 'Role_Latin', 'Role_Translation', 'Type', 'Specificity'],
        'foreign_keys': {},
        'related_tables': [
            {
                'junction_table': 'individual_social_roles',
                'junction_fk': 'Social_Role_ID',
                'target_fk': 'Individual_ID',
                'target_table': 'prosopography',
                'target_display': 'Full_Name_Latin',
                'label': 'Held By'
            },
            {
                'junction_table': 'role_honorific',
                'junction_fk': 'Role_ID',
                'target_fk': 'Honorific_ID',
                'target_table': 'honorifics',
                'target_display': 'Honorific',
                'label': 'Associated Honorifics'
            }
        ],
        'definitions_table': 'definitions',  # Special case: get definitions directly
        'definitions_fk': 'Social_Role_ID',
        'emoji': '💼'
    },
    'classical_sources': {
        'search_fields': ['Author_Nickname', 'Title_Nickname', 'Title_Arabic', 'Title_Translation', 
                         'Title_Latin', 'Author_Arabic', 'Author_Latin', 'Tags'],
        'display_fields': ['UID', 'Author_Nickname', 'Title_Nickname', 'Title_Arabic', 'Title_Translation',
                          'Title_Latin', 'Author_Arabic', 'Author_Latin', 'Tags', 
                          'Catalog', 'Century_Written', 'Date_Written_Hij'],
        'foreign_keys': {
            'Location_ID': {
                'table': 'gazetteer',
                'display_field': 'Nickname',
                'label': 'Location Written'
            }
        },
        'related_tables': [
            {
                'junction_table': 'references_to_classical_sources',
                'junction_fk': 'Classical_ID',
                'target_fk': 'Source_ID',
                'target_table': 'bibliography',
                'target_display': 'Title',
                'label': 'Referenced In'
            }
        ],
        'emoji': '📜'
    },
    'repositories': {
        'search_fields': ['Name_English', 'Name_Foreign', 'Acronym'],
        'display_fields': ['UID', 'Acronym', 'Name_English', 'Name_Foreign'],
        'foreign_keys': {
            'Location_ID': {
                'table': 'gazetteer',
                'display_field': 'Nickname',
                'label': 'Location'
            }
        },
        'emoji': '🏛️'
    },
    'honorifics': {
        'search_fields': ['Honorific', 'Translation'],
        'display_fields': ['UID', 'Honorific', 'Translation'],
        'foreign_keys': {},
        'related_tables': [
            {
                'junction_table': 'role_honorific',
                'junction_fk': 'Honorific_ID',
                'target_fk': 'Role_ID',
                'target_table': 'social_roles',
                'target_display': 'Role_Latin',
                'label': 'Associated Roles'
            }
        ],
        'emoji': '🎖️'
    },
    'knowledge_forms': {
        'search_fields': ['Name_Emic', 'Name_Latin', 'Translation'],
        'display_fields': ['UID', 'Name_Emic', 'Name_Latin', 'Translation', 'Equivalency'],
        'foreign_keys': {},
        'emoji': '🎓'
    },
    'epochs': {
        'search_fields': ['Epoch_Name'],
        'display_fields': ['UID', 'Epoch_Name', 'Start_Date_Greg', 'End_Date_Greg'],
        'foreign_keys': {},
        'emoji': '📅'
    },
    'definitions': {
        'search_fields': ['Definition', 'Type'],
        'display_fields': ['UID', 'Definition', 'Type'],
        'foreign_keys': {
            'Lexicon_ID': {
                'table': 'lexicon',
                'display_field': 'Term',
                'label': 'Term'
            },
            'Social_Role_ID': {
                'table': 'social_roles',
                'display_field': 'Role_Latin',
                'label': 'Social Role'
            },
            'Source_ID': {
                'table': 'bibliography',
                'display_field': 'Author',
                'label': 'Source'
            }
        },
        'emoji': '📝'
    },
    'seals': {
        'search_fields': ['Text'],
        'display_fields': ['UID', 'Text', 'Date_Hij'],
        'foreign_keys': {
            'Individual_ID': {
                'table': 'prosopography',
                'display_field': 'Full_Name_Latin',
                'label': 'Owner'
            },
            'Source_ID': {
                'table': 'bibliography',
                'display_field': 'Author',
                'label': 'Source'
            }
        },
        'emoji': '🔱'
    },
    'copies_holdings': {
        'search_fields': [],
        'display_fields': ['UID', 'Transcription_Date_Greg', 'Century'],
        'foreign_keys': {
            'Repository_ID': {
                'table': 'repositories',
                'display_field': 'Acronym',
                'label': 'Repository'
            },
            'Copied_Source_ID': {
                'table': 'bibliography',
                'display_field': 'Title',
                'label': 'Copied Work'
            },
            'Copied_Classical_ID': {
                'table': 'classical_sources',
                'display_field': 'Title_Nickname',
                'label': 'Classical Work'
            },
            'Scribe_Individual_ID': {
                'table': 'prosopography',
                'display_field': 'Full_Name_Latin',
                'label': 'Scribe'
            }
        },
        'emoji': '📄'
    },
    'location_attributes': {
        'search_fields': ['Type'],
        'display_fields': ['UID', 'Type', 'Start_Date_Greg', 'End_Date_Greg'],
        'foreign_keys': {
            'Location_ID': {
                'table': 'gazetteer',
                'display_field': 'Nickname',
                'label': 'Location'
            },
            'Source_ID': {
                'table': 'bibliography',
                'display_field': 'Author',
                'label': 'Source'
            }
        },
        'emoji': '📋'
    },
    'itineraries': {
        'search_fields': ['Purpose'],
        'display_fields': ['UID', 'Arrival_Date_Greg', 'Departure_Date_Greg', 'Purpose'],
        'foreign_keys': {
            'Individual_ID': {
                'table': 'prosopography',
                'display_field': 'Full_Name_Latin',
                'label': 'Traveler'
            },
            'Location_ID': {
                'table': 'gazetteer',
                'display_field': 'Nickname',
                'label': 'Location'
            },
            'Source_ID': {
                'table': 'bibliography',
                'display_field': 'Author',
                'label': 'Source'
            }
        },
        'emoji': '🗺️'
    },
}


def gen_search(search_term, table_name=None, max_results=20, include_notes=None):
    """
    General-purpose regex search across any database table.
    
    Args:
        search_term (str): Regex pattern to search for
        table_name (str or tuple, optional): Table(s) to search. If None, prompts user.
        max_results (int): Maximum results per table (default: 20)
        include_notes (bool, optional): Include Notes/Description fields in search. 
                                       If None, auto-prompts if initial search yields <5 results
    
    Examples:
        gen_search('محمد')                    # Prompts for table selection
        gen_search('محمد', 'prosopography')   # Search prosopography table
        gen_search('محمد', ('lexicon', 'prosopography'))  # Search multiple tables
        gen_search('rare_term', 'lexicon', include_notes=True)  # Force include Notes
    """
    conn = sqlite3.connect(database_path)
    _register_regex(conn)
    cursor = conn.cursor()
    
    try:
        # Get all available tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        all_tables = [row[0] for row in cursor.fetchall()]
        
        # Handle table selection
        if table_name is None:
            print("\n📋 Available tables:")
            for i, table in enumerate(all_tables, 1):
                config = TABLE_SEARCH_CONFIG.get(table)
                emoji = config['emoji'] if config else '📁'
                print(f"  {i}. {emoji} {table}")
            print(f"  {len(all_tables) + 1}. 🔍 ALL TABLES")
            
            choice = input("\nEnter table number (or 'q' to quit): ").strip()
            
            if choice.lower() == 'q':
                return
            
            try:
                choice_num = int(choice)
                if choice_num == len(all_tables) + 1:
                    table_name = tuple(all_tables)
                elif 1 <= choice_num <= len(all_tables):
                    table_name = all_tables[choice_num - 1]
                else:
                    print("❌ Invalid selection")
                    return
            except ValueError:
                print("❌ Invalid input")
                return
        
        # Convert single table to tuple for uniform handling
        if isinstance(table_name, str):
            table_name = (table_name,)
        
        # First pass: search without Notes fields
        total_results = 0
        tables_with_results = {}
        
        for table in table_name:
            if table not in all_tables:
                print(f"❌ Table '{table}' not found")
                continue
            
            # Get search configuration
            config = TABLE_SEARCH_CONFIG.get(table)
            if config is None:
                config = _auto_detect_search_config(cursor, table)
                print(f"⚠️  Using auto-detected configuration for '{table}'")
            
            # Determine which fields to search
            search_fields = config['search_fields'].copy()
            notes_fields = _get_notes_fields(search_fields)
            
            # If include_notes is explicitly False or None (first pass), exclude Notes fields
            if include_notes is False or include_notes is None:
                search_fields = [f for f in search_fields if f not in notes_fields]
            
            if not search_fields:
                continue
            
            # Validate that search fields exist in the table
            cursor.execute(f"PRAGMA table_info({table});")
            table_columns = [col[1] for col in cursor.fetchall()]
            valid_search_fields = [f for f in search_fields if f in table_columns]
            
            if not valid_search_fields:
                # Skip this table if none of the search fields exist
                continue
            
            # Build search query with escaped column names (backticks for SQL reserved words)
            search_conditions = ' OR '.join([f"`{field}` REGEXP ?" for field in valid_search_fields])
            search_params = [search_term] * len(valid_search_fields)
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM {table} WHERE {search_conditions};"
            cursor.execute(count_query, search_params)
            table_total = cursor.fetchone()[0]
            
            if table_total > 0:
                tables_with_results[table] = {
                    'config': config,
                    'count': table_total,
                    'notes_fields': notes_fields
                }
                total_results += table_total
        
        # If fewer than 5 results and include_notes not explicitly set, offer to search Notes
        if total_results < 5 and include_notes is None and any(
            tables_with_results[t]['notes_fields'] for t in tables_with_results
        ):
            print(f"\n⚠️  Only {total_results} results found in main fields.")
            response = input("🔍 Search Notes/Description fields too? (y/n): ").strip().lower()
            if response == 'y':
                include_notes = True
                # Recursively call with include_notes=True
                cursor.close()
                conn.close()
                return gen_search(search_term, table_name, max_results, include_notes=True)
        
        # Now perform full search and display
        print(f"\n🔍 Searching for: '{search_term}'")
        if include_notes:
            print("   Including Notes/Description fields")
        print("=" * 80)
        
        displayed_results = 0
        
        for table in table_name:
            if table not in tables_with_results:
                continue
            
            config = tables_with_results[table]['config']
            
            # Rebuild search with or without notes as needed
            search_fields = config['search_fields'].copy()
            if not include_notes:
                notes_fields = tables_with_results[table]['notes_fields']
                search_fields = [f for f in search_fields if f not in notes_fields]
            
            # Validate search fields exist
            cursor.execute(f"PRAGMA table_info({table});")
            table_columns = [col[1] for col in cursor.fetchall()]
            valid_search_fields = [f for f in search_fields if f in table_columns]
            
            if not valid_search_fields:
                continue
            
            search_conditions = ' OR '.join([f"`{field}` REGEXP ?" for field in valid_search_fields])
            search_params = [search_term] * len(valid_search_fields)
            
            # Validate display fields and filter to only existing ones
            valid_display_fields = [f for f in config['display_fields'] if f in table_columns]
            if not valid_display_fields:
                valid_display_fields = ['UID']  # Fallback to just UID
            
            # Get results with limit - escape column names with backticks
            display_cols = ', '.join([f"`{f}`" for f in valid_display_fields])
            query = f"""
                SELECT {display_cols}
                FROM {table}
                WHERE {search_conditions}
                LIMIT ?;
            """
            cursor.execute(query, search_params + [max_results])
            results = cursor.fetchall()
            
            if not results:
                continue
            
            # Display results
            print(f"\n{config['emoji']} {table.upper()} (showing {len(results)} of {tables_with_results[table]['count']} matches)")
            print("=" * 80)
            
            for i, row in enumerate(results, 1):
                # Create display dictionary
                result_dict = dict(zip(valid_display_fields, row))
                uid = result_dict.get('UID')
                
                # Display main identifier
                main_field = valid_display_fields[1] if len(valid_display_fields) > 1 else valid_display_fields[0]
                print(f"{i}. {result_dict.get(main_field, 'N/A')} (UID: {uid})")
                
                # Display other fields
                for field in valid_display_fields[2:]:  # Skip UID and main field
                    value = result_dict.get(field)
                    if value:
                        # Truncate long text fields
                        if isinstance(value, str) and len(value) > 100:
                            value = value[:100] + "..."
                        print(f"   📝 {field}: {value}")
                
                # Resolve foreign keys
                for fk_field, fk_config in config['foreign_keys'].items():
                    fk_value = result_dict.get(fk_field)
                    if fk_value:
                        fk_query = f"SELECT {fk_config['display_field']} FROM {fk_config['table']} WHERE UID = ?;"
                        cursor.execute(fk_query, (fk_value,))
                        fk_result = cursor.fetchone()
                        if fk_result:
                            print(f"   🔗 {fk_config['label']}: {fk_result[0]}")
                
                # Check for relationships (if this is a table with relationship joins)
                if 'related_tables' in config and config['related_tables']:
                    _display_related_records(cursor, table, uid, config['related_tables'])
                
                # Check for definitions (special case for lexicon and social_roles)
                if 'definitions_table' in config:
                    _display_definitions(cursor, uid, config['definitions_table'], config['definitions_fk'])
                
                # Check for location attributes (special case for gazetteer)
                if 'location_attributes_table' in config:
                    _display_location_attributes(cursor, uid)
                
                print()
            
            displayed_results += len(results)
        
        print("=" * 80)
        print(f"📊 SUMMARY: {displayed_results} results displayed, {total_results} total matches across {len(tables_with_results)} table(s)")
        
    except Exception as e:
        print(f"❌ Search error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()


"""
Citation Function

Generates formatted citations from bibliography entries, copies to clipboard,
and optionally produces a detailed markdown report saved to the Inbox folder.

Format varies by Type:
    archival_document : "Title," ACRONYM Catalog_No (Date).
    manuscript        : Author, Title ACRONYM Catalog_No, fols. X.
    other/published   : Author, Title (Date).

Special rule: if Catalog_No contains 'i126' and has exactly 3 hyphens,
the final segment is extracted as a folio reference:
    i126-1-529-23  →  i126-1-529, fol. 23

Requires pyperclip for clipboard support (pip install pyperclip).
Add 'import pyperclip' to the imports block at the top of this file.

Usage:
    cite()           # prompt for UIDs or search term, print + copy citation
    cite(True)       # report mode: extended metadata + markdown file saved to Inbox
    cite("report")   # same as cite(True)
"""


def _is_type(bib_type, match):
    """
    Flexible type matching: normalises underscores/spaces and strips whitespace
    before checking whether match appears anywhere in bib_type.

    Examples:
        _is_type('archival document', 'archival_document') → True
        _is_type('archival_document', 'archival_document') → True
        _is_type(' Archival_Document ', 'archival_document') → True

    Args:
        bib_type (str or None): Raw value from the Type field.
        match (str): The normalised string to look for.

    Returns:
        bool
    """
    if not bib_type:
        return False
    normalised = str(bib_type).strip().lower().replace('_', ' ')
    match_norm = match.strip().lower().replace('_', ' ')
    return match_norm in normalised


# Types that use archival citation format: "Title," ACRONYM Catalog_No (Date).
# Extend this list as new custom type values appear in your database.
ARCHIVAL_TYPES = {
    'archival_document',
    'archival document',
    'parent_delo',
}


def _format_folios(folios):
    """
    Format the Folios field into a citation-ready string.

    Rules:
        Pure integer (e.g. "45")    → "45 fols."
        Contains hyphen ("33a-35b") → "fols. 33a-35b"
        Any other string            → "fols. <value>"

    Returns:
        str: Formatted folio string, or "" if no value.
    """
    if not folios:
        return ""
    s = str(folios).strip()
    if not s:
        return ""
    try:
        int(s)
        return f"{s} fols."
    except ValueError:
        return f"fols. {s}"


def _parse_catalog_no(catalog_no):
    """
    Parse a catalog number, applying the i126 folio-extraction rule.

    If the value contains 'i126' AND has exactly 3 hyphens (i.e. 4 segments),
    strip the last segment and return it as a folio reference.

    Examples:
        'i126-1-529'    → ('i126-1-529', None)       # 2 hyphens, no change
        'i126-1-529-23' → ('i126-1-529', 'fol. 23')  # 3 hyphens, extract last

    Args:
        catalog_no (str or None): Raw value from the Catalog_No field.

    Returns:
        tuple: (catalog_str, folio_str_or_None)
    """
    if not catalog_no:
        return catalog_no, None

    s = str(catalog_no).strip()

    if 'i126' in s and s.count('-') == 3:
        base, fol_num = s.rsplit('-', 1)  # split off last segment only
        return base, f"fol. {fol_num}"

    return s, None


def _format_citation_string(author, title, date_greg, date_hij,
                             catalog_no, bib_type, folios, acronym,
                             markdown=False):
    """
    Build a citation string for one bibliography entry.

    Citation format depends on Type:
        archival_document : "Title," ACRONYM Catalog_No (Date).
        manuscript        : Author, Title ACRONYM Catalog_No, fols. X.
        other/published   : Author, Title (Date).

    Type matching is flexible: 'archival document' and 'archival_document'
    both match, and leading/trailing whitespace is ignored.

    Args:
        markdown (bool): If True, wrap manuscript/published titles in *...*
                         for markdown output. No effect on archival format.

    Returns:
        str: Formatted citation ending with a period.
    """
    # ── Normalise string inputs ───────────────────────────────────────────────
    author    = str(author).strip()    if author    else ""
    title     = str(title).strip()     if title     else ""
    date_greg = str(date_greg).strip() if date_greg else ""
    date_hij  = str(date_hij).strip()  if date_hij  else ""
    acronym   = str(acronym).strip()   if acronym   else ""

    # ── Date string ──────────────────────────────────────────────────────────
    if date_greg:
        date_str = f"({date_greg})"
    elif date_hij:
        date_str = f"({date_hij} h.)"
    else:
        date_str = ""

    # ── Catalog parsing ───────────────────────────────────────────────────────
    cat_str, cat_fol = _parse_catalog_no(catalog_no)
    cat_str = str(cat_str).strip() if cat_str else ""

    # ── Format by type ───────────────────────────────────────────────────────
    if any(_is_type(bib_type, t) for t in ARCHIVAL_TYPES):
        # "Title," ACRONYM Catalog_No (Date).
        archival = " ".join(p for p in [acronym, cat_str] if p)
        if cat_fol:
            archival = f"{archival}, {cat_fol}"

        parts = []
        if title:
            parts.append('"' + title + ',"')
        if archival:
            parts.append(archival)
        if date_str:
            parts.append(date_str)
        citation = " ".join(parts)

    else:
        # Manuscript or published source
        # Author, Title ACRONYM Catalog_No, fols. X.
        archival = " ".join(p for p in [acronym, cat_str] if p)

        parts = []
        if author:
            parts.append(author + ",")
        if title:
            title_fmt = f"*{title}*" if markdown else title
            parts.append(title_fmt)
        if archival:
            parts.append(archival)
        elif date_str:
            # Published source with no repository — date follows title
            parts.append(date_str)

        citation = " ".join(parts)

        # Append folio info — Folios field takes priority; i126 rule as fallback
        folio_str = _format_folios(folios)
        if not folio_str and cat_fol:
            folio_str = cat_fol
        if folio_str:
            citation = citation.rstrip() + f", {folio_str}"

    # Ensure exactly one trailing period
    citation = citation.rstrip()
    if not citation.endswith('.'):
        citation += '.'

    return citation


def _get_bib_records_for_cite(uids):
    """
    Fetch full bibliography records needed for citation, joined with repositories.

    Args:
        uids (list): Integer UIDs to fetch.

    Returns:
        list of dicts, one per matching UID, in the order UIDs were supplied.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    try:
        placeholders = ','.join(['?' for _ in uids])
        cursor.execute(f"""
            SELECT b.UID, b.Author, b.Title, b.Gloss,
                   b.Date_Pub_Greg, b.Date_Pub_Hij,
                   b.Catalog_No, b.Language, b.Status, b.Tags,
                   b.Notes, b.Type, b.Folios,
                   r.Acronym
            FROM bibliography b
            LEFT JOIN repositories r ON b.Repository_ID = r.UID
            WHERE b.UID IN ({placeholders})
        """, uids)

        cols = ['uid', 'author', 'title', 'gloss', 'date_greg', 'date_hij',
                'catalog_no', 'language', 'status', 'tags', 'notes',
                'bib_type', 'folios', 'acronym']
        rows = cursor.fetchall()

        # Preserve the requested UID order
        row_map = {row[0]: dict(zip(cols, row)) for row in rows}
        return [row_map[uid] for uid in uids if uid in row_map]

    finally:
        cursor.close()
        conn.close()


def _get_related_sources_for_cite(uid):
    """
    Get all related_sources entries for a given bibliography UID.

    Uses UNION ALL to get both directions in one query:
        - Rows where this doc is the referencing source → it "references" another
        - Rows where this doc is the referenced source  → it is "referenced by" another

    Args:
        uid (int): Bibliography UID to look up.

    Returns:
        list of dicts with keys:
            direction      ('references' | 'referenced_by')
            rel_type       (str or None)
            rel_notes      (str or None)
            other_uid      (int)
            other_author   (str or None)
            other_title    (str or None)
            other_catalog  (str or None)
            other_type     (str or None)
            other_acronym  (str or None)
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT 'references'    AS direction,
                   rs.Type, rs.Notes,
                   b2.UID, b2.Author, b2.Title, b2.Catalog_No, b2.Type,
                   r2.Acronym
            FROM related_sources rs
            JOIN bibliography b2      ON rs.Referenced_Source_ID  = b2.UID
            LEFT JOIN repositories r2 ON b2.Repository_ID         = r2.UID
            WHERE rs.Referencing_Source_ID = ?

            UNION ALL

            SELECT 'referenced_by' AS direction,
                   rs.Type, rs.Notes,
                   b2.UID, b2.Author, b2.Title, b2.Catalog_No, b2.Type,
                   r2.Acronym
            FROM related_sources rs
            JOIN bibliography b2      ON rs.Referencing_Source_ID = b2.UID
            LEFT JOIN repositories r2 ON b2.Repository_ID         = r2.UID
            WHERE rs.Referenced_Source_ID = ?
        """, (uid, uid))

        cols = ['direction', 'rel_type', 'rel_notes', 'other_uid',
                'other_author', 'other_title', 'other_catalog',
                'other_type', 'other_acronym']
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    finally:
        cursor.close()
        conn.close()


def _tokenize_for_report(value):
    """
    Split a space-or-linebreak-delimited field and rejoin with ", " for display.

    Used for Type, Tags, and Status fields in reports.
    e.g. "edited facsimile\\ntranscription" → "edited, facsimile, transcription"

    Returns:
        str: Comma-space-joined tokens, or "" if no value.
    """
    if not value:
        return ""
    tokens = [t.strip() for t in re.split(r'[\s\n\r]+', str(value)) if t.strip()]
    return ", ".join(tokens)


def _build_cite_report(records, markdown_citations):
    """
    Build, print, and save a detailed markdown citation report to the Inbox.
    Copies to clipboard if 10 or fewer entries.

    Args:
        records (list): Record dicts from _get_bib_records_for_cite().
        markdown_citations (list): Parallel formatted citation strings (markdown).
    """
    try:
        import pyperclip
        clipboard_available = True
    except ImportError:
        clipboard_available = False
        print("⚠️  pyperclip not installed — clipboard copy unavailable")
        print("   Install with: pip install pyperclip")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"cite_report_{timestamp}.md"
    filepath  = os.path.join(inbox_path, filename)

    lines = []
    lines.append("# Citation Report")
    lines.append(f"*Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}*\n")

    print("\n" + "=" * 70)
    print("📄 REPORT")
    print("=" * 70)

    for i, (rec, m_cite) in enumerate(zip(records, markdown_citations)):
        uid = rec['uid']

        # ── Citation line ─────────────────────────────────────────────────
        lines.append(m_cite)
        print(f"\n{m_cite}")

        # ── Bullet-point metadata ─────────────────────────────────────────
        bullets = []
        if rec['gloss']:
            bullets.append(f"- **Gloss:** {rec['gloss']}")
        if rec['language']:
            bullets.append(f"- **Language:** {rec['language']}")
        if rec['bib_type']:
            bullets.append(f"- **Type:** {_tokenize_for_report(rec['bib_type'])}")
        if rec['tags']:
            bullets.append(f"- **Tags:** {_tokenize_for_report(rec['tags'])}")
        if rec['status']:
            bullets.append(f"- **Status:** {_tokenize_for_report(rec['status'])}")
        if rec['notes']:
            bullets.append(f"- **Notes:** {rec['notes']}")

        for b in bullets:
            lines.append(b)
            print("  " + b.replace("**", ""))  # Plain text for terminal

        # ── Related documents ─────────────────────────────────────────────
        related = _get_related_sources_for_cite(uid)

        if related:
            lines.append("- **Related Documents:**")
            print("  Related Documents:")

            for r in related:
                other_cite = _format_citation_string(
                    r['other_author'], r['other_title'],
                    None, None,
                    r['other_catalog'], r['other_type'],
                    None, r['other_acronym'],
                    markdown=True
                )
                direction_label = "References"    if r['direction'] == 'references' \
                             else "Referenced by"
                type_str  = f" [{r['rel_type']}]"  if r['rel_type']  else ""
                notes_str = f" — {r['rel_notes']}"  if r['rel_notes'] else ""

                rel_line = f"  - {direction_label}{type_str}: {other_cite}{notes_str}"
                lines.append(rel_line)
                print("    " + rel_line.strip().replace("**", ""))

        # Horizontal rule between entries (not after the last one)
        if i < len(records) - 1:
            lines.append("\n---\n")
            print("\n" + "-" * 70)

    # ── Save markdown file ────────────────────────────────────────────────────
    markdown_text = "\n".join(lines)

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_text)
        print(f"\n✅ Report saved: {filepath}")
    except Exception as e:
        print(f"\n❌ Failed to save report: {e}")

    # ── Clipboard (report mode: only if ≤10 entries) ─────────────────────────
    if clipboard_available and len(records) <= 10:
        try:
            pyperclip.copy(markdown_text)
            print("✅ Report copied to clipboard")
        except Exception as e:
            print(f"⚠️  Clipboard copy failed: {e}")


def cite(report=False):
    """
    Generate citations from bibliography entries and copy to clipboard.

    Prompts for one or more bibliography UIDs (comma-separated), or a search
    term across Author, Title, Gloss, Notes, and Catalog_No.

    Citation format depends on Type:
        archival_document : "Title," ACRONYM Catalog_No (Date).
        manuscript        : Author, Title ACRONYM Catalog_No, fols. X.
        other/published   : Author, Title (Date).

    Multiple entries are separated by semicolons in the output.

    Args:
        report (bool or str): If truthy, saves a detailed markdown report to
                              the Inbox folder and prints extended metadata.
                              Clipboard copy limited to ≤10 entries in report
                              mode. Both cite(True) and cite("report") work.

    Returns:
        str: The formatted citation string, or None on failure.

    Examples:
        cite()          # Interactive prompt, plain citation + clipboard
        cite(True)      # Report mode
        cite("report")  # Also report mode
    """
    try:
        import pyperclip
        clipboard_available = True
    except ImportError:
        clipboard_available = False
        print("⚠️  pyperclip not installed — clipboard copy unavailable")
        print("   Install with: pip install pyperclip")

    # ── 1. Prompt for UIDs or search term ────────────────────────────────────
    print("\n" + "=" * 70)
    print("📚 CITE")
    print("=" * 70)
    print("Enter UID(s) separated by commas, or a search term")
    print("(searches Author / Title / Gloss / Notes / Catalog_No)\n")

    raw = input("UID(s) or search: ").strip()
    if not raw:
        print("❌ Nothing entered.")
        return None

    # Detect UID-only input: digits, spaces, and commas exclusively
    uid_pattern = re.compile(r'^\d+(\s*,\s*\d+)*$')

    if uid_pattern.match(raw):
        # ── Direct UID entry ──────────────────────────────────────────────
        uids = [int(u.strip()) for u in raw.split(',')]

    else:
        # ── Search mode ───────────────────────────────────────────────────
        conn_s = sqlite3.connect(database_path)
        _register_regex(conn_s)
        c = conn_s.cursor()

        try:
            c.execute("""
                SELECT b.UID, b.Author, b.Title, b.Catalog_No
                FROM bibliography b
                WHERE b.Author     REGEXP ?
                   OR b.Title      REGEXP ?
                   OR b.Gloss      REGEXP ?
                   OR b.Notes      REGEXP ?
                   OR b.Catalog_No REGEXP ?
                ORDER BY b.Author, b.Title
                LIMIT 30
            """, (raw, raw, raw, raw, raw))
            results = c.fetchall()
        finally:
            c.close()
            conn_s.close()

        if not results:
            print(f"❌ No matches for '{raw}'")
            return None

        print(f"\nFound {len(results)} results:")
        for i, (uid, author, title, catalog) in enumerate(results, 1):
            author_s  = (author[:20] + "...") if author  and len(author)  > 20 else (author  or "")
            title_s   = (title[:35]  + "...") if title   and len(title)   > 35 else (title   or "")
            catalog_s = catalog or ""
            print(f"  {i:2d}. [{uid}] {author_s} | {title_s} | {catalog_s}")

        print("\nSelect entries (space-separated numbers, e.g. '1 3'), or 'a' for all:")
        selection = input("Selection: ").strip().lower()

        if not selection:
            print("❌ Cancelled.")
            return None

        if selection == 'a':
            uids = [row[0] for row in results]
        else:
            uids = []
            for num in selection.split():
                if num.isdigit():
                    idx = int(num) - 1
                    if 0 <= idx < len(results):
                        uids.append(results[idx][0])
                    else:
                        print(f"   ⚠️  Skipping invalid number: {num}")

        if not uids:
            print("❌ No entries selected.")
            return None

    # ── 2. Fetch records ──────────────────────────────────────────────────────
    records = _get_bib_records_for_cite(uids)

    if not records:
        print("❌ No records found for those UIDs.")
        return None

    # ── 3. Build citation strings (terminal + markdown versions) ──────────────
    terminal_citations = []
    markdown_citations = []

    for rec in records:
        terminal_citations.append(_format_citation_string(
            rec['author'], rec['title'], rec['date_greg'], rec['date_hij'],
            rec['catalog_no'], rec['bib_type'], rec['folios'], rec['acronym'],
            markdown=False
        ))
        markdown_citations.append(_format_citation_string(
            rec['author'], rec['title'], rec['date_greg'], rec['date_hij'],
            rec['catalog_no'], rec['bib_type'], rec['folios'], rec['acronym'],
            markdown=True
        ))

    # Multiple entries joined by semicolon-space
    terminal_output = "; ".join(terminal_citations)

    # ── 4. Print citation ─────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("📋 CITATION")
    print("=" * 70)
    print(terminal_output)

    # ── 5. Clipboard (base mode always; report mode handled in report fn) ─────
    if not report and clipboard_available:
        try:
            pyperclip.copy(terminal_output)
            print("\n✅ Copied to clipboard")
        except Exception as e:
            print(f"\n⚠️  Clipboard copy failed: {e}")

    # ── 6. Report mode ────────────────────────────────────────────────────────
    if report:
        _build_cite_report(records, markdown_citations)

    return terminal_output