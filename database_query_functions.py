#!/usr/bin/env python3
"""
Library of functions for querying Eurasia database
"""

import sqlite3, os
import pandas as pd
import re
from tabulate import tabulate
"""
Setting up the database, confirming connection, and listing tables.
"""

#set home directory path
hdir = os.path.expanduser('~')

dh_path = '/Dropbox/Active_Directories/Digital_Humanities/'

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

def word_search(search_term, format_type="pandas"):
    """
    Search for terms in the lexicon table using regex and return results with definitions.
    
    Args:
        search_term (str): Regex pattern to search for
        format_type (str): "pandas" (default), "table" (grid), or "markdown"
    """
    # Establish a connection to the database
    conn = sqlite3.connect(database_path)
    _register_regex(conn)  # Register the regex function
    cursor = conn.cursor()
    
    try:
        # SQL query to search through multiple columns in the lexicon table and join on definitions
        query = """
        SELECT l.UID, l.Term, l.Translation, l.Emic_Term, l.Colonial_Term, l.Transliteration, d.Definition
        FROM lexicon l
        JOIN definitions d ON l.UID = d.Lexicon_ID
        WHERE l.Term REGEXP ? OR l.Translation REGEXP ? OR l.Emic_Term REGEXP ? OR l.Colonial_Term REGEXP ? OR l.Transliteration REGEXP ?;
        """
        
        # Execute the query with the search term
        cursor.execute(query, (search_term, search_term, search_term, search_term, search_term))
        
        # Fetch all results
        results = cursor.fetchall()
        
        # Create a DataFrame with labeled columns
        columns = ['UID', 'Term', 'Translation', 'Emic_Term', 'Colonial_Term', 'Transliteration', 'Definition']
        df = pd.DataFrame(results, columns=columns)
        
        # Set display options for long text
        pd.set_option('display.max_colwidth', None)  # Show full content of the Definition column
        
        # Initialize Related_Terms column
        df['Related_Terms'] = ''
        
        # Check for related terms
        for index, row in df.iterrows():
            uid = row['UID']
            # Query to find related terms
            related_query = """
            SELECT l.Term
            FROM related_terms rt
            JOIN lexicon l ON rt.Child_ID = l.UID
            WHERE rt.Parent_ID = ?;
            """
            cursor.execute(related_query, (uid,))
            related_terms = cursor.fetchall()
            # Concatenate related terms into a single string
            related_terms_list = [term[0] for term in related_terms]
            df.at[index, 'Related_Terms'] = ', '.join(related_terms_list)
        
    except Exception as e:
        print(f"Error during query execution: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

    _configure_display()  # Set display options
    
    # NEW: Add formatting before return
    if format_type == "table":
        print("\n📚 Lexicon Search Results:")
        print("=" * 50)
        print(tabulate(df, headers=df.columns, tablefmt='grid', showindex=False))
    elif format_type == "markdown":
        print(tabulate(df, headers=df.columns, tablefmt='pipe', showindex=False))
    
    return df

# Example usage
# search_results_df = word_search('your_regex_pattern')
# print(search_results_df)




def location_search(search_term):
    # Establish a connection to the database
    conn = sqlite3.connect(database_path)
    _register_regex(conn)  # Register the regex function
    cursor = conn.cursor()
    
    try:
        # SQL query to search through multiple columns in the gazetteer table
        query = """
        SELECT UID, Nickname, Location_Name_Arabic, Location_Name_Colonial, Location_Name_Latin
        FROM gazetteer
        WHERE Nickname REGEXP ? OR Location_Name_Arabic REGEXP ? OR Location_Name_Colonial REGEXP ? OR Location_Name_Latin REGEXP ?;
        """
        
        # Execute the query with the search term
        cursor.execute(query, (search_term, search_term, search_term, search_term))
        
        # Fetch all matching records
        matching_records = cursor.fetchall()
        
        # If no matches found, return an empty DataFrame
        if not matching_records:
            return pd.DataFrame()  # Return an empty DataFrame if no matches
        
        # Extract UIDs from the results
        uid_list = [record[0] for record in matching_records]
        
        # SQL query to join with location_attributes based on Location_ID
        attributes_query = """
        SELECT la.*, g.Nickname, g.Location_Name_Arabic, g.Location_Name_Colonial, g.Location_Name_Latin
        FROM location_attributes la
        JOIN gazetteer g ON la.Location_ID = g.UID
        WHERE g.UID IN ({});
        """.format(','.join('?' * len(uid_list)))  # Create placeholders for the UIDs
        
        # Execute the query with the list of UIDs
        cursor.execute(attributes_query, uid_list)
        
        # Fetch all results
        results = cursor.fetchall()
        
        # Create a DataFrame with all columns from location_attributes and additional columns from gazetteer
        columns = [description[0] for description in cursor.description]  # Get column names
        df = pd.DataFrame(results, columns=columns)
        
    except Exception as e:
        print(f"Error during query execution: {e}")
        df = pd.DataFrame()  # Return an empty DataFrame on error
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
    
    _configure_display()  # Set display options
    
    return df

# Example usage
# location_results_df = location_search('your_regex_pattern')
# print(location_results_df)

def bibliography_search(search_term, repository_filter=None):
    """Search for bibliography entries with optional repository filtering."""
    # Establish a connection to the database
    conn = sqlite3.connect(database_path)
    _register_regex(conn)  # Register the regex function
    cursor = conn.cursor()
    
    try:
        if repository_filter is None:
            # Simple search without repository filtering
            query = """
            SELECT b.UID, r.Acronym, b.Catalog_No, b.Author, b.Title, b.Date_Pub_Greg, b.Date_Pub_Hij
            FROM bibliography b
            LEFT JOIN repositories r ON b.Repository_ID = r.UID
            WHERE b.Author REGEXP ? OR b.Title REGEXP ?;
            """
            cursor.execute(query, (search_term, search_term))
            
        elif isinstance(repository_filter, int):
            # Filter by exact Repository_ID
            query = """
            SELECT b.UID, r.Acronym, b.Catalog_No, b.Author, b.Title, b.Date_Pub_Greg, b.Date_Pub_Hij
            FROM bibliography b
            LEFT JOIN repositories r ON b.Repository_ID = r.UID
            WHERE (b.Author REGEXP ? OR b.Title REGEXP ?) AND b.Repository_ID = ?;
            """
            cursor.execute(query, (search_term, search_term, repository_filter))
            
        else:
            # Filter by repository name/acronym (string search)
            query = """
            SELECT b.UID, r.Acronym, b.Catalog_No, b.Author, b.Title, b.Date_Pub_Greg, b.Date_Pub_Hij
            FROM bibliography b
            JOIN repositories r ON b.Repository_ID = r.UID
            WHERE (b.Author REGEXP ? OR b.Title REGEXP ?) 
            AND (r.Acronym REGEXP ? OR r.Name_Foreign REGEXP ? OR r.Name_English REGEXP ?);
            """
            cursor.execute(query, (search_term, search_term, repository_filter, repository_filter, repository_filter))
        
        # Fetch all results
        results = cursor.fetchall()
        
        # Create a DataFrame with specified columns
        columns = ['UID', 'Acronym', 'Catalog_No', 'Author', 'Title', 'Date_Pub_Greg', 'Date_Pub_Hij']
        df = pd.DataFrame(results, columns=columns)
        
        # If no matches found, return empty DataFrame
        if df.empty:
            print(f"No bibliography entries found for search term: '{search_term}'")
            if repository_filter:
                print(f"with repository filter: '{repository_filter}'")
        
    except Exception as e:
        print(f"Error during query execution: {e}")
        df = pd.DataFrame()  # Return empty DataFrame on error
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
    
    _configure_display()  # Set display options
    return df