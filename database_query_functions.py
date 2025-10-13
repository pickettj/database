#!/usr/bin/env python3
"""
Library of functions for querying Eurasia database
"""

import sqlite3, os
import pandas as pd
import re
from datetime import datetime

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


def bibliography_search(search_term, repository_filter=None, max_results=None, save_report=False):
    """
    Search for bibliography entries and show related sources and references.
    
    Args:
        search_term (str): Regex pattern to search for in Author or Title
        repository_filter (int or str, optional): Repository ID or name/acronym to filter by
        max_results (int, optional): Maximum number of results to display per section (default: None = unlimited)
        save_report (bool): If True, saves results as markdown report to Inbox
    """
    conn = sqlite3.connect(database_path)
    _register_regex(conn)
    cursor = conn.cursor()

    print(f"🔍 Searching for: '{search_term}'" + (f" (showing up to {max_results} results per section)" if max_results else ""))
    if repository_filter:
        print(f"   Repository filter: '{repository_filter}'")
    print("=" * 80)

    try:
        # Build query based on repository filter
        if repository_filter is None:
            query = """
                SELECT b.UID, b.Author, b.Title, b.Date_Pub_Greg, b.Date_Pub_Hij, 
                       r.Acronym, r.Name_English, b.Catalog_No
                FROM bibliography b
                LEFT JOIN repositories r ON b.Repository_ID = r.UID
                WHERE b.Author REGEXP ? OR b.Title REGEXP ?
            """
            count_query = """
                SELECT COUNT(*)
                FROM bibliography b
                LEFT JOIN repositories r ON b.Repository_ID = r.UID
                WHERE b.Author REGEXP ? OR b.Title REGEXP ?
            """
            params = (search_term, search_term)
        elif isinstance(repository_filter, int):
            query = """
                SELECT b.UID, b.Author, b.Title, b.Date_Pub_Greg, b.Date_Pub_Hij,
                       r.Acronym, r.Name_English, b.Catalog_No
                FROM bibliography b
                LEFT JOIN repositories r ON b.Repository_ID = r.UID
                WHERE (b.Author REGEXP ? OR b.Title REGEXP ?) AND b.Repository_ID = ?
            """
            count_query = """
                SELECT COUNT(*)
                FROM bibliography b
                LEFT JOIN repositories r ON b.Repository_ID = r.UID
                WHERE (b.Author REGEXP ? OR b.Title REGEXP ?) AND b.Repository_ID = ?
            """
            params = (search_term, search_term, repository_filter)
        else:
            query = """
                SELECT b.UID, b.Author, b.Title, b.Date_Pub_Greg, b.Date_Pub_Hij,
                       r.Acronym, r.Name_English, b.Catalog_No
                FROM bibliography b
                JOIN repositories r ON b.Repository_ID = r.UID
                WHERE (b.Author REGEXP ? OR b.Title REGEXP ?) 
                AND (r.Acronym REGEXP ? OR r.Name_Foreign REGEXP ? OR r.Name_English REGEXP ?)
            """
            count_query = """
                SELECT COUNT(*)
                FROM bibliography b
                JOIN repositories r ON b.Repository_ID = r.UID
                WHERE (b.Author REGEXP ? OR b.Title REGEXP ?) 
                AND (r.Acronym REGEXP ? OR r.Name_Foreign REGEXP ? OR r.Name_English REGEXP ?)
            """
            params = (search_term, search_term, repository_filter, repository_filter, repository_filter)

        # Get total count
        cursor.execute(count_query, params)
        bibliography_total = cursor.fetchone()[0]

        # Get results with limit
        cursor.execute(query + (" LIMIT ?" if max_results else ""), 
                      params + ((max_results,) if max_results else ()))
        bibliography_results = cursor.fetchall()
        matched_uids = [row[0] for row in bibliography_results]

        print(f"📚 BIBLIOGRAPHY ENTRIES (displaying {len(bibliography_results)} out of {bibliography_total} matches)")
        print("-" * 40)
        
        if bibliography_results:
            for i, (uid, author, title, date_greg, date_hij, acronym, repo_name, catalog) in enumerate(bibliography_results, 1):
                print(f"{i}. {author} - {title}")
                if acronym:
                    print(f"   🏛️ Repository: {acronym}" + (f" ({repo_name})" if repo_name else ""))
                if catalog:
                    print(f"   📋 Catalog: {catalog}")
                if date_greg:
                    print(f"   📅 Date (Gregorian): {date_greg}")
                if date_hij:
                    print(f"   📅 Date (Hijri): {date_hij}")
                print()
        else:
            print("   No matches found\n")

        # Get related sources if we have matches
        if matched_uids:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM related_sources
                WHERE Referencing_Source_ID IN ({','.join(['?' for _ in matched_uids])})
                   OR Referenced_Source_ID IN ({','.join(['?' for _ in matched_uids])});
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
                WHERE rs.Referencing_Source_ID IN ({','.join(['?' for _ in matched_uids])})
                   OR rs.Referenced_Source_ID IN ({','.join(['?' for _ in matched_uids])})
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
        related_count = len(related_sources) if matched_uids else 0
        print(f"📊 SUMMARY: {entries_count} bibliography entries, {related_count} related sources")

    except Exception as e:
        print(f"❌ Search error: {e}")
    finally:
        cursor.close()
        conn.close()