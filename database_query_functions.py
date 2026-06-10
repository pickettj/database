#!/usr/bin/env python3
"""
Library of functions for querying Eurasia database
"""

import sqlite3, os
import pandas as pd
import re
from datetime import datetime
import pyperclip
import json
import subprocess

"""
Setting up the database, confirming connection, and listing tables.
"""

#set home directory path
hdir = os.path.expanduser('~')

dh_path = '/Dropbox/Active_Directories/Digital_Humanities/'
inbox_path = os.path.join(hdir, 'Dropbox/Active_Directories/Inbox')
custom_table_exports_path = os.path.join(
    hdir, 'Dropbox/Active_Directories/Digital_Humanities/Datasets/custom_table_exports'
)

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

"""
FK Resolution and Browse Configuration
"""

# Global FK resolution config: whenever a column with this name appears in any
# table, resolve it by joining to the specified table and pulling the display field.
# Per-table overrides live in BROWSE_CONFIG['table']['fk_overrides'].
FK_DISPLAY_CONFIG = {
    'Repository_ID':          {'table': 'repositories',      'field': 'Acronym',         'label': 'Repository'},
    'Author_ID':              {'table': 'prosopography',      'field': 'Nickname_Latin',  'label': 'Author'},
    'Source_ID':              {'table': 'bibliography',       'field': 'Title',           'label': 'Source'},
    'Individual_ID':          {'table': 'prosopography',      'field': 'Nickname_Latin',  'label': 'Person'},
    'Location_ID':            {'table': 'gazetteer',          'field': 'Nickname',        'label': 'Location'},
    'Classical_ID':           {'table': 'classical_sources',  'field': 'Title_Nickname',  'label': 'Classical Source'},
    'Social_Role_ID':         {'table': 'social_roles',       'field': 'Role_Latin',      'label': 'Role'},
    'Lexicon_ID':             {'table': 'lexicon',            'field': 'Term',            'label': 'Term'},
    'Knowledge_Form_ID':      {'table': 'knowledge_forms',    'field': 'Name_Latin',      'label': 'Knowledge Form'},
    'Copied_Source_ID':       {'table': 'bibliography',       'field': 'Title',           'label': 'Copied Source'},
    'Copied_Classical_ID':    {'table': 'classical_sources',  'field': 'Title_Nickname',  'label': 'Copied Classical'},
    'Scribe_Individual_ID':   {'table': 'prosopography',      'field': 'Nickname_Latin',  'label': 'Scribe'},
    'Reference_Source_ID':    {'table': 'bibliography',       'field': 'Title',           'label': 'Reference Source'},
    'Honorific_ID':           {'table': 'honorifics',         'field': 'Honorific',       'label': 'Honorific'},
    'Role_ID':                {'table': 'social_roles',       'field': 'Role_Latin',      'label': 'Role'},
    'Conquest_ID':            {'table': 'conquests',          'field': 'UID',             'label': 'Conquest'},
    'Conquering_Power_ID':    {'table': 'prosopography',      'field': 'Nickname_Latin',  'label': 'Conquering Power'},
    'Defending_Power_ID':     {'table': 'prosopography',      'field': 'Nickname_Latin',  'label': 'Defending Power'},
    'Conquered_Territory_ID': {'table': 'gazetteer',          'field': 'Nickname',        'label': 'Conquered Territory'},
    'Commentating_Work':      {'table': 'classical_sources',  'field': 'Title_Nickname',  'label': 'Commentating Work'},
    'Commentated_Work':       {'table': 'classical_sources',  'field': 'Title_Nickname',  'label': 'Commentated Work'},
    'Referencing_Source_ID':  {'table': 'bibliography',       'field': 'Title',           'label': 'Referencing Source'},
    'Referenced_Source_ID':   {'table': 'bibliography',       'field': 'Title',           'label': 'Referenced Source'},
    'Tertiary_ID':            {'table': 'gazetteer',          'field': 'Nickname',        'label': 'Tertiary Location'},
    'Parent':                 {'table': 'prosopography',      'field': 'Nickname_Latin',  'label': 'Parent Person'},
    'Child':                  {'table': 'prosopography',      'field': 'Nickname_Latin',  'label': 'Child Person'},
    # Context-dependent FKs (Parent_ID / Child_ID) are resolved via PRAGMA at runtime
    # and can be overridden per table in BROWSE_CONFIG fk_overrides
}

# Per-table browse configuration: priority display fields and FK display overrides.
# Priority fields are always shown by default; user can add extras interactively.
BROWSE_CONFIG = {
    'bibliography': {
        'priority_fields': ['UID', 'Author', 'Title', 'Gloss', 'Date_Pub_Greg',
                            'Date_Pub_Hij', 'Language', 'Type', 'Tags', 'Status',
                            'Catalog_No', 'Repository_ID'],
        'fk_overrides': {}
    },
    'prosopography': {
        'priority_fields': ['UID', 'Full_Name_Arabic', 'Full_Name_Latin', 'Nickname_Latin',
                            'Birthdate_Greg', 'Deathdate_Greg', 'Social_Role'],
        'fk_overrides': {}
    },
    'gazetteer': {
        'priority_fields': ['UID', 'Nickname', 'Location_Name_Arabic', 'Location_Name_Colonial',
                            'Location_Name_Latin', 'Type'],
        'fk_overrides': {}
    },
    'lexicon': {
        'priority_fields': ['UID', 'Term', 'Translation', 'Emic_Term', 'Transliteration',
                            'Etymology', 'Scope', 'Tags'],
        'fk_overrides': {}
    },
    'social_roles': {
        'priority_fields': ['UID', 'Role_Emic', 'Role_Latin', 'Role_Translation',
                            'Type', 'Specificity'],
        'fk_overrides': {}
    },
    'classical_sources': {
        'priority_fields': ['UID', 'Author_Nickname', 'Title_Nickname', 'Title_Arabic',
                            'Title_Translation', 'Author_Arabic', 'Century_Written',
                            'Date_Written_Hij', 'Tags'],
        'fk_overrides': {}
    },
    'repositories': {
        'priority_fields': ['UID', 'Acronym', 'Name_English', 'Name_Foreign', 'Location_ID'],
        'fk_overrides': {}
    },
    'copies_holdings': {
        'priority_fields': ['UID', 'Repository_ID', 'Copied_Source_ID', 'Copied_Classical_ID',
                            'Scribe_Individual_ID', 'Transcription_Date_Greg', 'Century'],
        'fk_overrides': {
            # Show full name instead of Acronym for this table
            'Repository_ID': {'table': 'repositories', 'field': 'Name_English', 'label': 'Repository'}
        }
    },
    'definitions': {
        'priority_fields': ['UID', 'Definition', 'Type', 'Lexicon_ID', 'Social_Role_ID',
                            'Source_ID', 'Page_No'],
        'fk_overrides': {}
    },
    'related_sources': {
        'priority_fields': ['UID', 'Referencing_Source_ID', 'Referenced_Source_ID', 'Type', 'Notes'],
        'fk_overrides': {}
    },
    'related_terms': {
        'priority_fields': ['UID', 'Parent_ID', 'Child_ID', 'Type', 'Source_ID'],
        'fk_overrides': {
            'Parent_ID': {'table': 'lexicon', 'field': 'Term', 'label': 'Parent Term'},
            'Child_ID':  {'table': 'lexicon', 'field': 'Term', 'label': 'Child Term'},
        }
    },
    'individual_social_roles': {
        'priority_fields': ['UID', 'Individual_ID', 'Social_Role_ID', 'Source_ID'],
        'fk_overrides': {}
    },
    'references_to_individuals': {
        'priority_fields': ['UID', 'Individual_ID', 'Source_ID'],
        'fk_overrides': {}
    },
    'references_to_locations': {
        'priority_fields': ['UID', 'Location_ID', 'Source_ID'],
        'fk_overrides': {}
    },
    'references_to_classical_sources': {
        'priority_fields': ['UID', 'Classical_ID', 'Source_ID'],
        'fk_overrides': {}
    },
    'location_attributes': {
        'priority_fields': ['UID', 'Location_ID', 'Type', 'Source_ID',
                            'Start_Date_Greg', 'End_Date_Greg'],
        'fk_overrides': {}
    },
    'location_hierarchies': {
        'priority_fields': ['UID', 'Parent_ID', 'Child_ID', 'Relationship', 'Source_ID'],
        'fk_overrides': {
            'Parent_ID': {'table': 'gazetteer', 'field': 'Nickname', 'label': 'Parent Location'},
            'Child_ID':  {'table': 'gazetteer', 'field': 'Nickname', 'label': 'Child Location'},
        }
    },
    'honorifics': {
        'priority_fields': ['UID', 'Honorific', 'Translation'],
        'fk_overrides': {}
    },
    'role_honorific': {
        'priority_fields': ['UID', 'Role_ID', 'Honorific_ID', 'Source_ID'],
        'fk_overrides': {}
    },
    'knowledge_forms': {
        'priority_fields': ['UID', 'Name_Emic', 'Name_Latin', 'Translation', 'Equivalency'],
        'fk_overrides': {}
    },
    'knowledge_branch': {
        'priority_fields': ['UID', 'Parent_ID', 'Child_ID', 'Classical_ID', 'Source_ID'],
        'fk_overrides': {
            'Parent_ID': {'table': 'knowledge_forms', 'field': 'Name_Latin', 'label': 'Parent Form'},
            'Child_ID':  {'table': 'knowledge_forms', 'field': 'Name_Latin', 'label': 'Child Form'},
        }
    },
    'knowledge_mastery': {
        'priority_fields': ['UID', 'Individual_ID', 'Knowledge_Form_ID', 'Source_ID'],
        'fk_overrides': {}
    },
    'itineraries': {
        'priority_fields': ['UID', 'Individual_ID', 'Location_ID', 'Source_ID',
                            'Arrival_Date_Greg', 'Departure_Date_Greg', 'Purpose'],
        'fk_overrides': {}
    },
    'epochs': {
        'priority_fields': ['UID', 'Epoch_Name', 'Start_Date_Greg', 'End_Date_Greg'],
        'fk_overrides': {}
    },
    'seals': {
        'priority_fields': ['UID', 'Individual_ID', 'Source_ID', 'Text', 'Date_Hij'],
        'fk_overrides': {}
    },
    'prices': {
        'priority_fields': ['UID', 'Source_ID', 'Location_ID', 'Date_Greg'],
        'fk_overrides': {}
    },
    'conquests': {
        'priority_fields': ['UID', 'Conquering_Power_ID', 'Defending_Power_ID',
                            'Conquered_Territory_ID'],
        'fk_overrides': {}
    },
    'relationships': {
        'priority_fields': ['UID', 'Parent', 'Child', 'Source_ID'],
        'fk_overrides': {}
    },
    'commentaries': {
        'priority_fields': ['UID', 'Commentating_Work', 'Commentated_Work'],
        'fk_overrides': {}
    },
    'classical_genre': {
        'priority_fields': ['UID', 'Classical_ID', 'Knowledge_Form_ID', 'Source_ID'],
        'fk_overrides': {}
    },
    'multiple_sources_conquests': {
        'priority_fields': ['UID', 'Source_ID', 'Conquest_ID'],
        'fk_overrides': {}
    },
}

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
and optionally produces a detailed markdown report or note file saved to the
Inbox folder.

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
    cite("report")   # extended metadata + markdown report saved to Inbox
    cite("note")     # single-document note file saved to Inbox
"""


def _is_type(bib_type, match):
    """
    Flexible type matching: normalises underscores/spaces and strips whitespace
    before checking whether match appears anywhere in bib_type.

    Examples:
        _is_type('archival document', 'archival_document') → True
        _is_type('archival_document', 'archival_document') → True
        _is_type(' Archival_Document ', 'archival_document') → True
    """
    if not bib_type:
        return False
    normalised = str(bib_type).strip().lower().replace('_', ' ')
    match_norm = match.strip().lower().replace('_', ' ')
    return match_norm in normalised


# Types that use archival citation format: "Title," ACRONYM Catalog_No (Date).
# Extend this set as new custom type values appear in your database.
ARCHIVAL_TYPES = {
    'archival_document',
    'archival document',
    'parent_delo',
}


def _normalize_filename_component(text, space_char='_'):
    """
    Normalize a string for safe use in a filename.

    Steps:
        1. NFKD decomposition: splits characters like ā into base letter + combining mark
        2. Strip combining marks (diacritics): ā → a, ī → i, etc.
        3. Encode to ASCII, dropping anything with no ASCII equivalent
           (Arabic script, other non-Latin characters are silently removed)
        4. Replace spaces with space_char ('_' for titles, '-' for acronyms/call nos)
        5. Strip any remaining characters that are not alphanumeric, hyphen, or underscore

    Args:
        text (str): Raw field value.
        space_char (str): Character to substitute for spaces.

    Returns:
        str: Filename-safe string.

    Examples:
        _normalize_filename_component("Nigaristān-i Āṣafī")  → "Nigaristan-i_Asafi"
        _normalize_filename_component("O'zR MA", '-')         → "OzR-MA"
        _normalize_filename_component("i126-1-529", '-')      → "i126-1-529"
    """
    import unicodedata

    if not text:
        return ""

    # Step 1 & 2: decompose and strip combining marks
    nfkd = unicodedata.normalize('NFKD', str(text))
    stripped = ''.join(c for c in nfkd if not unicodedata.combining(c))

    # Step 3: drop anything non-ASCII
    ascii_text = stripped.encode('ascii', 'ignore').decode('ascii')

    # Step 4: spaces → space_char
    ascii_text = ascii_text.replace(' ', space_char)

    # Step 4b: slashes → hyphens (preserves call number structure e.g. IOR/F/4 → IOR-F-4)
    ascii_text = ascii_text.replace('/', '-')

    # Step 5: keep only alphanumeric, hyphen, underscore, period
    ascii_text = re.sub(r'[^\w\-.]', '', ascii_text)

    return ascii_text.strip('_-.')


def _format_folios(folios):
    """
    Format the Folios field into a citation-ready string.

    Rules:
        Pure integer (e.g. "45")    → "45 fols."
        Contains hyphen ("33a-35b") → "fols. 33a-35b"
        Any other string            → "fols. <value>"
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
        'i126-1-529'    → ('i126-1-529', None)
        'i126-1-529-23' → ('i126-1-529', 'fol. 23')
    """
    if not catalog_no:
        return catalog_no, None

    s = str(catalog_no).strip()

    if 'i126' in s and s.count('-') == 3:
        base, fol_num = s.rsplit('-', 1)
        return base, f"fol. {fol_num}"

    return s, None

def _is_real_date(val):
    """
    Return True if val is a whole number (directly entered, not calculated).
    Calculated/derived dates are stored with a decimal component from the old
    auto-conversion system.
    """
    if val is None:
        return False
    try:
        f = float(val)
        return f == int(f)
    except (ValueError, TypeError):
        return False


def _date_to_int(val):
    """Convert a date value to integer, or None if not convertible."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _hij_to_greg_range(hij_int):
    """
    Convert a Hijri year to an approximate Gregorian range string.
    Formula: G ≈ H × 0.97 + 622
    e.g. 1317 → "1899–1900"
    """
    g = hij_int * 0.97 + 622
    g_int = int(g)
    return f"{g_int}–{g_int + 1}"


def _greg_to_hij_range(greg_int):
    """
    Convert a Gregorian year to an approximate Hijri range string.
    Formula: H ≈ (G - 622) / 0.97
    e.g. 1899 → "1317–1318"
    """
    h = (greg_int - 622) / 0.97
    h_int = int(h)
    return f"{h_int}–{h_int + 1}"


def _format_date_bullet(label, greg_val, hij_val):
    """
    Format a date bullet point, determining which value is real vs calculated.

    A "real" date is a whole number (directly entered by the researcher).
    A "calculated" date has a decimal component (derived by old auto-conversion).

    Cases:
        Both real      → "Date: 1899 / 1317 h."          (precise equivalence)
        Greg real only → "Date: 1899 (ca. 1317–1318 h.)"
        Hij real only  → "Date: 1317 h. (ca. 1899–1900)"
        Neither real   → None (omit bullet entirely)

    Args:
        label (str): e.g. "Date" or "Date Scribed"
        greg_val: Raw value from a _Greg field
        hij_val:  Raw value from a _Hij field

    Returns:
        str: Markdown bullet string, or None if no usable date.
    """
    greg_real = _is_real_date(greg_val)
    hij_real  = _is_real_date(hij_val)
    greg_int  = _date_to_int(greg_val)
    hij_int   = _date_to_int(hij_val)

    if not greg_real and not hij_real:
        return None

    if greg_real and hij_real:
        return f"- **{label}:** {greg_int} / {hij_int} h."

    if greg_real:
        hij_range = _greg_to_hij_range(greg_int)
        return f"- **{label}:** {greg_int} (ca. {hij_range} h.)"

    # Hij real only
    greg_range = _hij_to_greg_range(hij_int)
    return f"- **{label}:** {hij_int} h. (ca. {greg_range})"


def _format_citation_string(author, title, date_greg, date_hij,
                             catalog_no, bib_type, folios, acronym,
                             markdown=False):
    """
    Build a citation string for one bibliography entry.

    Citation format depends on Type:
        archival_document : "Title," ACRONYM Catalog_No (Date).
        manuscript        : Author, Title, Acronym, Catalog_No, fols. X (Date).
        other/published   : Author, Title, Acronym, Catalog_No (Date).

    Only real (whole-number) dates appear in the citation line.
    Calculated/decimal dates are suppressed here; they appear in report bullets.

    Args:
        markdown (bool): If True, wrap manuscript/published titles in *...*
                         for markdown output. No effect on archival format.
    """
    # ── Normalise string inputs ───────────────────────────────────────────────
    author  = str(author).strip()  if author  else ""
    title   = str(title).strip()   if title   else ""
    acronym = str(acronym).strip() if acronym else ""

    # ── Date string (real dates only) ────────────────────────────────────────
    if _is_real_date(date_greg):
        date_str = f"({_date_to_int(date_greg)})"
    elif _is_real_date(date_hij):
        date_str = f"({_date_to_int(date_hij)} h.)"
    else:
        date_str = ""

    # ── Catalog parsing ───────────────────────────────────────────────────────
    cat_str, cat_fol = _parse_catalog_no(catalog_no)
    cat_str = str(cat_str).strip() if cat_str else ""

    # ── Format by type ────────────────────────────────────────────────────────
    if any(_is_type(bib_type, t) for t in ARCHIVAL_TYPES):
        # "Title," ACRONYM Catalog_No (Date).
        # No commas between acronym and call number for archival documents —
        # they run together as a single archival reference string.
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
        # Manuscript or published source.
        # All components comma-separated:
        # Author, Title, Acronym, Catalog_No, fols. X (Date).
        parts = []
        if author:
            parts.append(author)
        if title:
            parts.append(f"*{title}*" if markdown else title)
        if acronym:
            parts.append(acronym)
        if cat_str:
            parts.append(cat_str)

        citation = ", ".join(parts)

        # Folio info — Folios field takes priority; i126 rule as fallback
        folio_str = _format_folios(folios)
        if not folio_str and cat_fol:
            folio_str = cat_fol
        if folio_str:
            citation = citation.rstrip() + f", {folio_str}"

        # Date in parentheses at the end
        if date_str:
            citation = citation.rstrip() + f" {date_str}"

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
                   b.Date_Scribed_Greg, b.Date_Scribed_Hij,
                   b.Catalog_No, b.Language, b.Status, b.Tags,
                   b.Notes, b.Type, b.Folios,
                   r.Acronym
            FROM bibliography b
            LEFT JOIN repositories r ON b.Repository_ID = r.UID
            WHERE b.UID IN ({placeholders})
        """, uids)

        cols = ['uid', 'author', 'title', 'gloss',
                'date_greg', 'date_hij',
                'date_scribed_greg', 'date_scribed_hij',
                'catalog_no', 'language', 'status', 'tags', 'notes',
                'bib_type', 'folios', 'acronym']
        rows = cursor.fetchall()

        row_map = {row[0]: dict(zip(cols, row)) for row in rows}
        return [row_map[uid] for uid in uids if uid in row_map]

    finally:
        cursor.close()
        conn.close()


def _get_related_sources_for_cite(uid):
    """
    Get all related_sources entries for a given bibliography UID, both directions.

    Uses UNION ALL to get:
        - Rows where this doc is the referencing source → it "references" another
        - Rows where this doc is the referenced source  → it is "referenced by" another
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
    e.g. "edited facsimile\\ntranscription" → "edited, facsimile, transcription"
    """
    if not value:
        return ""
    tokens = [t.strip() for t in re.split(r'[\s\n\r]+', str(value)) if t.strip()]
    return ", ".join(tokens)


def _build_metadata_bullets(rec, include_uid=True):
    """
    Build metadata bullet-point lines for a bibliography record.
    Used by both report and note modes.

    Bullet order: UID, Date, Date Scribed, Gloss, Language, Type, Tags, Status, Notes

    Args:
        rec (dict): Record dict from _get_bib_records_for_cite().
        include_uid (bool): Whether to include UID as the first bullet.

    Returns:
        tuple: (markdown_lines list, terminal_lines list)
    """
    md_bullets   = []
    term_bullets = []

    def _add(md_line):
        md_bullets.append(md_line)
        term_bullets.append("  " + md_line.replace("**", ""))

    if include_uid:
        _add(f"- **UID:** {rec['uid']}")

    # Publication date
    date_bullet = _format_date_bullet("Date",
                                      rec.get('date_greg'),
                                      rec.get('date_hij'))
    if date_bullet:
        _add(date_bullet)

    # Scribal date
    scribed_bullet = _format_date_bullet("Date Scribed",
                                         rec.get('date_scribed_greg'),
                                         rec.get('date_scribed_hij'))
    if scribed_bullet:
        _add(scribed_bullet)

    if rec.get('gloss'):
        _add(f"- **Gloss:** {rec['gloss']}")
    if rec.get('language'):
        _add(f"- **Language:** {rec['language']}")
    if rec.get('bib_type'):
        _add(f"- **Type:** {_tokenize_for_report(rec['bib_type'])}")
    if rec.get('tags'):
        _add(f"- **Tags:** {_tokenize_for_report(rec['tags'])}")
    if rec.get('status'):
        _add(f"- **Status:** {_tokenize_for_report(rec['status'])}")
    if rec.get('notes'):
        _add(f"- **Notes:** {rec['notes']}")

    return md_bullets, term_bullets


def _build_related_bullets(uid):
    """
    Build related-document bullet lines for a bibliography record.

    Returns:
        tuple: (markdown_lines list, terminal_lines list)
        Both are empty lists if no related documents exist.
    """
    related = _get_related_sources_for_cite(uid)
    if not related:
        return [], []

    md_lines   = ["- **Related Documents:**"]
    term_lines = ["  Related Documents:"]

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
        md_lines.append(rel_line)
        term_lines.append("    " + rel_line.strip())

    return md_lines, term_lines


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

        # Citation line
        lines.append(m_cite)
        print(f"\n{m_cite}")

        # Metadata bullets (UID first)
        md_bullets, term_bullets = _build_metadata_bullets(rec, include_uid=True)
        lines.extend(md_bullets)
        for b in term_bullets:
            print(b)

        # Related documents
        md_rel, term_rel = _build_related_bullets(uid)
        lines.extend(md_rel)
        for b in term_rel:
            print(b)

        # Horizontal rule between entries (not after the last one)
        if i < len(records) - 1:
            lines.append("\n---\n")
            print("\n" + "-" * 70)

    # Save markdown file
    markdown_text = "\n".join(lines)

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(markdown_text)
        print(f"\n✅ Report saved: {filepath}")
    except Exception as e:
        print(f"\n❌ Failed to save report: {e}")

    # Clipboard: report mode only if ≤10 entries
    if clipboard_available and len(records) <= 10:
        try:
            pyperclip.copy(markdown_text)
            print("✅ Report copied to clipboard")
        except Exception as e:
            print(f"⚠️  Clipboard copy failed: {e}")


def _build_cite_note(rec, terminal_citation, markdown_citation):
    """
    Build, print, and save a note file for a single bibliography entry.

    File name format: Title_Acronym_CatalogNo_serUID.md
        - Title component: spaces → '_', diacritics stripped
        - Acronym component: spaces → '-', diacritics stripped
        - CatalogNo component: spaces → '-', diacritics stripped
        - Falls back to CatalogNo alone if Title is empty

    File structure:
        # <citation>
        - **UID:** ...
        - **Gloss:** ...
        ... (standard metadata bullets)
        - **Related Documents:** ...

        ---

        [blank space for notes]

    Also copies the base citation to clipboard.

    Args:
        rec (dict): Single record dict from _get_bib_records_for_cite().
        terminal_citation (str): Plain-text citation string.
        markdown_citation (str): Markdown-formatted citation string.
    """
    try:
        import pyperclip
        clipboard_available = True
    except ImportError:
        clipboard_available = False
        print("⚠️  pyperclip not installed — clipboard copy unavailable")
        print("   Install with: pip install pyperclip")

    # ── Build filename ────────────────────────────────────────────────────────
    # Truncate title to first 4 words to keep filenames manageable
    title_raw    = ' '.join(rec['title'].split()[:4]) if rec['title'] else ""
    title_part   = _normalize_filename_component(title_raw, space_char='_')
    acronym_part = _normalize_filename_component(rec['acronym'],  space_char='-') if rec['acronym']  else ""
    catalog_part = _normalize_filename_component(rec['catalog_no'], space_char='-') if rec['catalog_no'] else ""
    uid_part     = f"ser{rec['uid']}"

    # Assemble non-empty components, always ending with serUID
    name_parts = [p for p in [title_part, acronym_part, catalog_part] if p]
    if not name_parts:
        # Absolute fallback: just the UID
        name_parts = [uid_part]
        filename = f"{uid_part}.md"
    else:
        filename = "_".join(name_parts) + f"_{uid_part}.md"

    filepath = os.path.join(inbox_path, filename)

    # ── Build file content ────────────────────────────────────────────────────
    lines = []

    # Heading: the citation itself
    lines.append(f"# {markdown_citation}\n")

    # Metadata bullets (UID first)
    md_bullets, term_bullets = _build_metadata_bullets(rec, include_uid=True)
    lines.extend(md_bullets)

    # Related documents
    md_rel, _ = _build_related_bullets(rec['uid'])
    lines.extend(md_rel)

    # Section break then blank space for writing
    lines.append("\n---\n")

    # ── Print to terminal ─────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("📝 NOTE FILE")
    print("=" * 70)
    print(f"\n{terminal_citation}\n")
    for b in term_bullets:
        print(b)

    # ── Save file ─────────────────────────────────────────────────────────────
    note_text = "\n".join(lines)

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(note_text)
        print(f"\n✅ Note file saved: {filepath}")
    except Exception as e:
        print(f"\n❌ Failed to save note file: {e}")

    # ── Clipboard: base citation ──────────────────────────────────────────────
    if clipboard_available:
        try:
            pyperclip.copy(terminal_citation)
            print("✅ Citation copied to clipboard")
        except Exception as e:
            print(f"⚠️  Clipboard copy failed: {e}")


def cite(report=False, note=False):
    """
    Generate citations from bibliography entries and copy to clipboard.

    Prompts for one or more bibliography UIDs (comma-separated), or a search
    term across Author, Title, Gloss, Notes, and Catalog_No.

    Citation format depends on Type:
        archival_document / parent_delo : "Title," ACRONYM Catalog_No (Date).
        manuscript                      : Author, Title ACRONYM Catalog_No, fols. X.
        other/published                 : Author, Title (Date).

    Multiple entries are separated by semicolons in plain citation output.
    Note mode accepts only a single entry.

    Args:
        report (bool or str): Save a detailed markdown report to the Inbox.
                              Both cite(True) and cite("report") work.
                              Clipboard copy limited to ≤10 entries.
        note (bool or str):   Save a single-document note file to the Inbox.
                              Both cite(note=True) and cite("note") work.
                              Errors if more than one entry is selected.

    Returns:
        str: The formatted citation string, or None on failure.

    Examples:
        cite()             # plain citation + clipboard
        cite("report")     # extended report
        cite(note=True)    # note file for single document
    """
    try:
        import pyperclip
        clipboard_available = True
    except ImportError:
        clipboard_available = False
        print("⚠️  pyperclip not installed — clipboard copy unavailable")
        print("   Install with: pip install pyperclip")

    # Support positional string arguments for both flags
    if report == "report":
        report = True
    if report == "note" or note == "note":
        note   = True
        report = False

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

    uid_pattern = re.compile(r'^\d+(\s*,\s*\d+)*$')

    if uid_pattern.match(raw):
        uids = [int(u.strip()) for u in raw.split(',')]

    else:
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

    # ── Note mode: enforce single entry ──────────────────────────────────────
    if note and len(uids) > 1:
        print(f"❌ Note mode only works with a single entry. {len(uids)} were selected.")
        print("   Re-run and select one entry only.")
        return None

    # ── 2. Fetch records ──────────────────────────────────────────────────────
    records = _get_bib_records_for_cite(uids)

    if not records:
        print("❌ No records found for those UIDs.")
        return None

    # ── 3. Build citation strings ─────────────────────────────────────────────
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

    terminal_output = "; ".join(terminal_citations)

    # ── 4. Print citation ─────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("📋 CITATION")
    print("=" * 70)
    print(terminal_output)

    # ── 5. Clipboard (base mode) ──────────────────────────────────────────────
    if not report and not note and clipboard_available:
        try:
            pyperclip.copy(terminal_output)
            print("\n✅ Copied to clipboard")
        except Exception as e:
            print(f"\n⚠️  Clipboard copy failed: {e}")

    # ── 6. Report mode ────────────────────────────────────────────────────────
    if report:
        _build_cite_report(records, markdown_citations)

    # ── 7. Note mode ──────────────────────────────────────────────────────────
    if note:
        _build_cite_note(records[0], terminal_citations[0], markdown_citations[0])

    return terminal_output

"""
Custom Table Browser
"""

# Columns whose values should be tokenized before display (split on whitespace/linebreak/comma)
TOKENIZED_FILTER_FIELDS = {'Tags', 'Type', 'Language', 'Status'}


def _prompt_field_value(field_name, col_type, display_options, sql_ref):
    """
    Prompt user for a filter value for a single field.

    For FK fields (numeric actual values): uses the actual ID as the selection
    key displayed, so user types the UID directly rather than an ordinal.

    For text fields: ordinal numbering, REGEXP match so selecting "scans"
    matches rows containing "scans" alongside other values.

    Args:
        field_name (str): Column name.
        col_type (str): SQL column type.
        display_options (list): List of (display_str, actual_value) tuples,
                                or plain values.
        sql_ref (str): SQL reference for WHERE clause.

    Returns:
        tuple: (condition_sql, param) or (None, None) if user cancels.
    """
    is_numeric = col_type.upper() in ('INTEGER', 'REAL', 'NUMERIC', 'INT')

    shown = display_options[:50] if display_options else []

    # Detect FK-style options: actual values are all integers (UIDs)
    fk_style = shown and all(
        isinstance((opt[1] if isinstance(opt, tuple) else opt), int)
        for opt in shown
    )

    if shown:
        print(f"\n  Values in {field_name}:")
        for opt in shown:
            if isinstance(opt, tuple):
                display_str, actual_val = opt
            else:
                display_str, actual_val = str(opt), opt

            if fk_style:
                # Use the UID itself as the entry key
                print(f"    {actual_val}. {display_str}")
            else:
                # Ordinal numbering for text/tokenized fields
                print(f"    {shown.index(opt) + 1:3d}. {display_str}")

        if len(display_options) > 50:
            print(f"         ... ({len(display_options)} total, showing first 50)")

        print(f"\n  Enter a selection key to choose, or type a custom {'value' if is_numeric else 'regex pattern'}:")
    else:
        print(f"\n  Filter on {field_name}:")
        if not is_numeric:
            print(f"  (Supports regex)")

    val_input = input(f"  Value: ").strip()

    if not val_input:
        return None, None

    if val_input.isdigit() and shown:
        int_input = int(val_input)

        if fk_style:
            # Match by actual UID value
            match = next(
                (opt for opt in shown
                 if (opt[1] if isinstance(opt, tuple) else opt) == int_input),
                None
            )
            if match:
                actual_val = match[1] if isinstance(match, tuple) else match
                return f"{sql_ref} = ?", actual_val
            # Fall through to custom entry if no match found

        else:
            # Ordinal selection for text fields
            idx = int_input - 1
            if 0 <= idx < len(shown):
                opt = shown[idx]
                actual_val = opt[1] if isinstance(opt, tuple) else opt
                # REGEXP so "scans" matches "scans notes partial_transcription" etc.
                return f"{sql_ref} REGEXP ?", str(actual_val)

    # Custom entry
    if is_numeric:
        try:
            val = int(val_input) if '.' not in val_input else float(val_input)
            return f"{sql_ref} = ?", val
        except ValueError:
            print(f"  ⚠️  Expected a number, got '{val_input}' — skipping")
            return None, None
    else:
        return f"{sql_ref} REGEXP ?", val_input


def _get_field_display_options(cursor, table, col_name, col_type, fk_config=None):
    """
    Build display options for a field, handling tokenization and FK resolution.

    For tokenized fields (Tags, Type, Language, Status):
        Splits all stored values on whitespace/linebreak/comma, counts token
        frequency across all rows, returns sorted by frequency descending.
        Returns list of (display_str, token_str) tuples.

    For FK fields (fk_config provided):
        Fetches unique IDs with row counts, resolves display value from
        referenced table, returns "ID — DisplayValue (count)" formatted tuples.
        Returns list of (display_str, id_int) tuples.

    For all other fields:
        Unique values ordered by frequency descending.
        Returns list of plain values.

    Args:
        cursor: SQLite cursor.
        table (str): Home table name.
        col_name (str): Column name.
        col_type (str): SQL column type.
        fk_config (dict or None): FK resolution config from FK_DISPLAY_CONFIG.

    Returns:
        list: Display options (tuples or plain values).
    """
    # ── Tokenized fields ──────────────────────────────────────────────────────
    if col_name in TOKENIZED_FILTER_FIELDS:
        try:
            cursor.execute(
                f"SELECT {col_name} FROM {table} WHERE {col_name} IS NOT NULL"
            )
            rows = cursor.fetchall()
        except Exception:
            return []

        freq = {}
        for (val,) in rows:
            if val:
                # Split on whitespace (including \n \r \x0b) and commas
                tokens = re.split(r'[\s\n\r\x0b,]+', str(val))
                for tok in tokens:
                    tok = tok.strip()
                    if tok:
                        freq[tok] = freq.get(tok, 0) + 1

        # Sort by frequency descending
        sorted_tokens = sorted(freq.items(), key=lambda x: x[1], reverse=True)
        return [(f"{tok}  ({count})", tok) for tok, count in sorted_tokens]

    # ── FK fields ─────────────────────────────────────────────────────────────
    if fk_config and fk_config.get('table') and fk_config.get('field'):
        ref_table = fk_config['table']
        ref_field = fk_config['field']
        try:
            # Count rows per FK value in home table
            cursor.execute(f"""
                SELECT {table}.{col_name}, COUNT(*) as cnt, {ref_table}.{ref_field}
                FROM {table}
                LEFT JOIN {ref_table} ON {table}.{col_name} = {ref_table}.UID
                WHERE {table}.{col_name} IS NOT NULL
                GROUP BY {table}.{col_name}
                ORDER BY cnt DESC
                LIMIT 60
            """)
            rows = cursor.fetchall()
            opts = []
            for (id_val, count, display_val) in rows:
                display_val = display_val or '—'
                # Truncate long display values
                if isinstance(display_val, str) and len(display_val) > 45:
                    display_val = display_val[:45] + '…'
                opts.append((f"{id_val} — {display_val}  ({count})", id_val))
            return opts
        except Exception:
            pass

    # ── Regular fields ────────────────────────────────────────────────────────
    try:
        cursor.execute(f"""
            SELECT {col_name}, COUNT(*) as cnt
            FROM {table}
            WHERE {col_name} IS NOT NULL
            GROUP BY {col_name}
            ORDER BY cnt DESC
            LIMIT 60
        """)
        rows = cursor.fetchall()
        return [(f"{val}  ({count})", val) for val, count in rows]
    except Exception:
        return []


def _collect_filter_conditions(cursor, home_table, col_info, all_fk_in_table,
                                joins_needed, fk_to_alias, join_counter, round_label):
    """
    Interactive loop collecting filter conditions for one round (AND or OR).

    Presents two sections of filterable fields:
        1. All home table columns (FK columns show resolved display values)
        2. Resolved FK fields (filter by the joined table's display value)

    Mutates joins_needed and fk_to_alias in-place when a new FK join is
    needed for a filter that wasn't already registered for display.

    Args:
        cursor: SQLite cursor with REGEXP registered.
        home_table (str): Primary table name.
        col_info (list): [(col_name, col_type), ...] for all home table columns.
        all_fk_in_table (dict): {col_name: fk_config} for all FK columns.
        joins_needed (dict): Mutable — alias → join info.
        fk_to_alias (dict): Mutable — FK col name → alias.
        join_counter (list): Mutable single-element list [int] for alias generation.
        round_label (str): 'AND' or 'OR' for display.

    Returns:
        tuple: (conditions list, params list)
    """
    conditions = []
    params     = []

    # Build resolved FK filter options (filter by joined table's text field)
    fk_resolved_opts = []
    for fk_col, fk_config in all_fk_in_table.items():
        if fk_config.get('table') and fk_config.get('field') and fk_config['field'] != 'UID':
            fk_resolved_opts.append({
                'display': f"{fk_config['label']} ({fk_config['field']}) via {fk_col}",
                'fk_col':  fk_col,
                'config':  fk_config
            })

    while True:
        print(f"\n{'─' * 60}")
        print(f"  {round_label} filter — press Enter with no input to finish")
        print(f"{'─' * 60}")

        print(f"\n  Home table fields:")
        for i, (name, col_type) in enumerate(col_info, 1):
            fk_marker = " 🔗" if name in all_fk_in_table else ""
            print(f"    {i:3d}. {name} ({col_type}){fk_marker}")

        if fk_resolved_opts:
            offset = len(col_info)
            print(f"\n  Resolved FK fields (filter via joined table's text):")
            for i, opt in enumerate(fk_resolved_opts, offset + 1):
                print(f"    {i:3d}. {opt['display']}")

        total = len(col_info) + len(fk_resolved_opts)
        choice = input(f"\n  Select field (1–{total}): ").strip()

        if not choice:
            break

        if not choice.isdigit():
            print("  ❌ Invalid input")
            continue

        idx = int(choice) - 1
        if not (0 <= idx < total):
            print(f"  ❌ Out of range (1–{total})")
            continue

        if idx < len(col_info):
            # ── Home table field ──────────────────────────────────────────────
            col_name, col_type = col_info[idx]
            fk_config = all_fk_in_table.get(col_name)

            # Build display options — FK-aware and tokenization-aware
            display_options = _get_field_display_options(
                cursor, home_table, col_name, col_type, fk_config
            )

            cond, param = _prompt_field_value(
                col_name, col_type, display_options,
                f"{home_table}.{col_name}"
            )
            if cond:
                conditions.append(cond)
                params.append(param)

        else:
            # ── Resolved FK field (filter on joined table's text value) ───────
            opt       = fk_resolved_opts[idx - len(col_info)]
            fk_col    = opt['fk_col']
            fk_config = opt['config']
            ref_table = fk_config['table']
            ref_field = fk_config['field']

            # Register join if not already present
            if fk_col not in fk_to_alias:
                alias = f"j{join_counter[0]}"
                join_counter[0] += 1
                joins_needed[alias] = {
                    'fk_col':    fk_col,
                    'ref_table': ref_table,
                    'ref_field': ref_field,
                    'label':     fk_config['label']
                }
                fk_to_alias[fk_col] = alias
            else:
                alias = fk_to_alias[fk_col]

            # Fetch unique text values from the referenced table, ordered by frequency
            try:
                cursor.execute(f"""
                    SELECT {ref_table}.{ref_field}, COUNT(*) as cnt
                    FROM {home_table}
                    JOIN {ref_table} ON {home_table}.{fk_col} = {ref_table}.UID
                    WHERE {ref_table}.{ref_field} IS NOT NULL
                    GROUP BY {ref_table}.{ref_field}
                    ORDER BY cnt DESC
                    LIMIT 60
                """)
                rows = cursor.fetchall()
                display_options = [(f"{val}  ({count})", val) for val, count in rows]
            except Exception:
                display_options = []

            cond, param = _prompt_field_value(
                f"{fk_config['label']} ({ref_field})", 'TEXT',
                display_options,
                f"{alias}.{ref_field}"
            )
            if cond:
                conditions.append(cond)
                params.append(param)

        if conditions:
            print(f"  ✅ {len(conditions)} {round_label} condition(s) added so far")

    return conditions, params

def custom_table(table_name=None):
    """
    Interactive query builder: filter a database table with FK join resolution,
    then export the result to a parquet file and launch the Streamlit viewer.

    Flow:
        1. Select table
        2. Choose display fields (priority fields pre-selected; extras optional)
        3. Additional fields from linked tables (any column from any joined table)
        4. FK fields automatically resolve — both raw ID and display value included
        5. Round 1 filters: AND conditions (each narrows results)
        6. Round 2 filters: OR conditions (each broadens results, optional)
        7. Preview row count → confirm → export → launch Streamlit

    Args:
        table_name (str, optional): Table to query. Prompts if None.

    Returns:
        pd.DataFrame: Query result, or None if cancelled or empty.

    Examples:
        custom_table()                # prompts for table
        custom_table('bibliography')  # starts with bibliography
    """
    conn = sqlite3.connect(database_path)
    _register_regex(conn)
    cursor = conn.cursor()

    try:
        # ── 1. Table selection ────────────────────────────────────────────────
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        all_tables = [row[0] for row in cursor.fetchall()]

        if table_name is None:
            print("\n" + "=" * 70)
            print("📊 CUSTOM TABLE BROWSER")
            print("=" * 70)
            print("\nAvailable tables (✓ = priority fields configured):")
            for i, t in enumerate(all_tables, 1):
                marker = " ✓" if t in BROWSE_CONFIG else ""
                print(f"  {i:2d}. {t}{marker}")

            choice = input("\nSelect table (number or name): ").strip()

            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(all_tables):
                    table_name = all_tables[idx]
                else:
                    print("❌ Invalid selection")
                    return None
            elif choice in all_tables:
                table_name = choice
            else:
                print(f"❌ Table '{choice}' not found")
                return None

        if table_name not in all_tables:
            print(f"❌ Table '{table_name}' does not exist")
            return None

        print(f"\n📋 Table: {table_name}")

        # ── 2. Schema info ────────────────────────────────────────────────────
        cursor.execute(f"PRAGMA table_info({table_name})")
        raw_cols  = cursor.fetchall()
        col_info  = [(col[1], col[2]) for col in raw_cols]
        col_names = [c[0] for c in col_info]

        cursor.execute(f"PRAGMA foreign_key_list({table_name})")
        fk_raw = cursor.fetchall()

        fk_overrides = BROWSE_CONFIG.get(table_name, {}).get('fk_overrides', {})

        # Build FK map: col_name → resolved config
        all_fk_in_table = {}
        for fk in fk_raw:
            fk_col    = fk[3]
            ref_table = fk[2]
            config    = fk_overrides.get(fk_col) or FK_DISPLAY_CONFIG.get(fk_col)
            if config and config.get('table'):
                all_fk_in_table[fk_col] = config
            else:
                all_fk_in_table[fk_col] = {
                    'table': ref_table,
                    'field': 'UID',
                    'label': fk_col.replace('_', ' ')
                }

        # Catch unconstrained columns that appear in FK_DISPLAY_CONFIG
        for col_name in col_names:
            if col_name not in all_fk_in_table and col_name in FK_DISPLAY_CONFIG:
                config = FK_DISPLAY_CONFIG[col_name]
                if config.get('table'):
                    all_fk_in_table[col_name] = config

        # ── 3. Display field selection ────────────────────────────────────────
        priority = BROWSE_CONFIG.get(table_name, {}).get('priority_fields', col_names[:10])
        priority = [f for f in priority if f in col_names]

        print("\n" + "=" * 70)
        print("📊 DISPLAY FIELDS")
        print("=" * 70)
        print("Priority fields (always included):")
        for f in priority:
            if f in all_fk_in_table:
                cfg = all_fk_in_table[f]
                if cfg['field'] != 'UID':
                    print(f"  • {f}  → also pulling {cfg['label']} ({cfg['field']})")
                else:
                    print(f"  • {f}  🔗")
            else:
                print(f"  • {f}")

        extra_fields   = [c for c in col_names if c not in priority]
        selected_fields = list(priority)

        if extra_fields:
            print(f"\nAdditional home table fields (optional):")
            for i, field in enumerate(extra_fields, 1):
                fk_marker = f" 🔗 → {all_fk_in_table[field]['label']}" if field in all_fk_in_table else ""
                print(f"  {i:2d}. {field}{fk_marker}")

            print("\nEnter numbers to add (space-separated), or Enter to skip:")
            selection = input("Add fields: ").strip()

            if selection:
                for num in selection.split():
                    if num.isdigit():
                        idx = int(num) - 1
                        if 0 <= idx < len(extra_fields):
                            selected_fields.append(extra_fields[idx])

        # ── 3b. Additional fields from linked tables ───────────────────────────
        extra_join_fields = []  # list of dicts: {fk_col, ref_table, ref_field, col_label}

        linkable      = []
        seen_ref_tables = set()
        for fk_col, fk_cfg in all_fk_in_table.items():
            ref_table = fk_cfg.get('table')
            if ref_table and ref_table not in seen_ref_tables:
                linkable.append({
                    'fk_col':    fk_col,
                    'ref_table': ref_table,
                    'label':     fk_cfg.get('label', fk_col)
                })
                seen_ref_tables.add(ref_table)

        if linkable:
            print(f"\nAdditional fields from linked tables:")
            for i, lnk in enumerate(linkable, 1):
                print(f"  {i:2d}. {lnk['label']} table ({lnk['ref_table']}) via {lnk['fk_col']}")

            print("\nEnter linked table number to browse its fields (or Enter to skip):")

            while True:
                lnk_choice = input("  Linked table: ").strip()
                if not lnk_choice:
                    break
                if not lnk_choice.isdigit():
                    print("  ❌ Invalid input")
                    continue
                lnk_idx = int(lnk_choice) - 1
                if not (0 <= lnk_idx < len(linkable)):
                    print(f"  ❌ Out of range")
                    continue

                lnk       = linkable[lnk_idx]
                ref_table = lnk['ref_table']

                cursor.execute(f"PRAGMA table_info({ref_table})")
                ref_cols = [col[1] for col in cursor.fetchall()]

                default_field = all_fk_in_table[lnk['fk_col']].get('field', 'UID')

                print(f"\n  Columns in {ref_table} (default: {default_field}):")
                for i, col in enumerate(ref_cols, 1):
                    default_marker = " ✓ (already included)" if col == default_field else ""
                    print(f"    {i:3d}. {col}{default_marker}")

                print("\n  Enter column numbers to add (space-separated), or Enter to skip:")
                col_selection = input("  Add columns: ").strip()

                if col_selection:
                    for num in col_selection.split():
                        if num.isdigit():
                            cidx = int(num) - 1
                            if 0 <= cidx < len(ref_cols):
                                chosen_col = ref_cols[cidx]
                                if chosen_col == default_field:
                                    print(f"  ⚠️  {chosen_col} is already included — skipping")
                                    continue
                                extra_join_fields.append({
                                    'fk_col':    lnk['fk_col'],
                                    'ref_table': ref_table,
                                    'ref_field': chosen_col,
                                    'col_label': f"{lnk['label']}_{chosen_col}"
                                })
                                print(f"  ✅ Added {ref_table}.{chosen_col}")

                again = input("\n  Browse another linked table? (y/n): ").strip().lower()
                if again != 'y':
                    break

        print(f"\n✅ {len(selected_fields)} home fields + {len(extra_join_fields)} extra joined fields selected")

        # ── 4. Register FK joins for display ──────────────────────────────────
        joins_needed  = {}
        fk_to_alias   = {}
        join_counter  = [0]

        # Joins for default FK display fields
        for field in selected_fields:
            if field in all_fk_in_table:
                cfg = all_fk_in_table[field]
                if cfg.get('table') and cfg.get('field') and cfg['field'] != 'UID':
                    if field not in fk_to_alias:
                        alias = f"j{join_counter[0]}"
                        join_counter[0] += 1
                        joins_needed[alias] = {
                            'fk_col':    field,
                            'ref_table': cfg['table'],
                            'ref_field': cfg['field'],
                            'label':     cfg['label']
                        }
                        fk_to_alias[field] = alias

        # Joins for extra linked table fields (reuse alias if same FK col)
        for ejf in extra_join_fields:
            fk_col = ejf['fk_col']
            if fk_col not in fk_to_alias:
                alias = f"j{join_counter[0]}"
                join_counter[0] += 1
                joins_needed[alias] = {
                    'fk_col':    fk_col,
                    'ref_table': ejf['ref_table'],
                    'ref_field': ejf['ref_field'],
                    'label':     all_fk_in_table[fk_col]['label']
                }
                fk_to_alias[fk_col] = alias

        # ── 5. Filter conditions ──────────────────────────────────────────────
        print("\n" + "=" * 70)
        print("🔍 FILTER — Round 1: AND  (each condition narrows results)")
        print("=" * 70)
        print("Press Enter immediately to skip all filtering.\n")

        and_conditions, and_params = _collect_filter_conditions(
            cursor, table_name, col_info, all_fk_in_table,
            joins_needed, fk_to_alias, join_counter, "AND"
        )

        print("\n" + "=" * 70)
        print("🔍 FILTER — Round 2: OR  (each condition broadens results)")
        print("=" * 70)

        or_conditions, or_params = [], []
        add_or = input("Add OR conditions? (y/n): ").strip().lower()
        if add_or == 'y':
            or_conditions, or_params = _collect_filter_conditions(
                cursor, table_name, col_info, all_fk_in_table,
                joins_needed, fk_to_alias, join_counter, "OR"
            )

        # ── 6. Build SQL ──────────────────────────────────────────────────────
        select_parts     = []
        column_label_map = {}

        for field in selected_fields:
            select_parts.append(f"{table_name}.{field}")
            if field in fk_to_alias:
                alias        = fk_to_alias[field]
                info         = joins_needed[alias]
                resolved_col = f"{field}__{info['ref_field']}"
                select_parts.append(f"{alias}.{info['ref_field']} AS {resolved_col}")
                column_label_map[resolved_col] = info['label']

        # Extra joined columns
        for ejf in extra_join_fields:
            fk_col    = ejf['fk_col']
            ref_field = ejf['ref_field']
            col_label = ejf['col_label']
            if fk_col in fk_to_alias:
                alias        = fk_to_alias[fk_col]
                resolved_col = f"{fk_col}__{ref_field}"
                select_parts.append(f"{alias}.{ref_field} AS {resolved_col}")
                column_label_map[resolved_col] = col_label

        select_sql = ",\n    ".join(select_parts)

        # JOINs
        join_sql = ""
        for alias, info in joins_needed.items():
            join_sql += (
                f"\nLEFT JOIN {info['ref_table']} {alias} "
                f"ON {table_name}.{info['fk_col']} = {alias}.UID"
            )

        # WHERE
        all_params = []
        where_sql  = ""

        if and_conditions or or_conditions:
            parts = []
            if and_conditions:
                parts.append("(" + " AND ".join(and_conditions) + ")")
                all_params.extend(and_params)
            if or_conditions:
                or_block = "(" + " OR ".join(or_conditions) + ")"
                if parts:
                    parts.append("OR " + or_block)
                else:
                    parts.append(or_block)
                all_params.extend(or_params)
            where_sql = "WHERE " + " ".join(parts)

        query = f"SELECT\n    {select_sql}\nFROM {table_name}{join_sql}\n{where_sql}".strip()

        # ── 7. Execute ────────────────────────────────────────────────────────
        try:
            cursor.execute(query, all_params)
            rows       = cursor.fetchall()
            result_cols = [desc[0] for desc in cursor.description]
        except Exception as e:
            print(f"\n❌ Query error: {e}")
            print(f"Query:\n{query}")
            return None

        if not rows:
            print("\n⚠️  Query returned 0 rows — try adjusting your filters")
            return None

        df = pd.DataFrame(rows, columns=result_cols)

        print(f"\n✅ {len(df):,} rows × {len(df.columns)} columns")
        print(f"\nFirst 3 rows preview:")
        print(df.head(3).to_string(max_colwidth=40))

        confirm = input("\nExport and open in Streamlit viewer? (y/n): ").strip().lower()
        if confirm != 'y':
            print("❌ Cancelled — returning DataFrame without exporting")
            return df

        # ── 8. Export parquet + metadata JSON ─────────────────────────────────
        os.makedirs(custom_table_exports_path, exist_ok=True)

        ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name    = f"{table_name}_{ts}"
        parquet_path = os.path.join(custom_table_exports_path, f"{base_name}.parquet")
        meta_path    = os.path.join(custom_table_exports_path, f"{base_name}.json")

        df.to_parquet(parquet_path, index=False)

        filter_summary = []
        for cond in and_conditions:
            filter_summary.append(f"AND: {cond}")
        for cond in or_conditions:
            filter_summary.append(f"OR: {cond}")

        metadata = {
            'table':         table_name,
            'timestamp':     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'rows':          len(df),
            'columns':       list(df.columns),
            'filters':       filter_summary,
            'column_labels': column_label_map,
        }
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        print(f"✅ Exported: {parquet_path}")

        # ── 9. Launch Streamlit ────────────────────────────────────────────────
        browse_app_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'browse_app.py'
        )

        if not os.path.exists(browse_app_path):
            print(f"⚠️  browse_app.py not found at {browse_app_path}")
            print(f"   Create it and re-run, or open the parquet manually.")
            return df

        subprocess.Popen(
            ['streamlit', 'run', browse_app_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        print("✅ Streamlit viewer launching at http://localhost:8501")

        return df

    finally:
        cursor.close()
        conn.close()