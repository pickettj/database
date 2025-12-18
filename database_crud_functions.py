#!/usr/bin/env python3
"""
Eurasia database "CRUD helper" (Create, Read, Update, Delete).
"""

import sqlite3, os
import pandas as pd
import re
from datetime import datetime

"""
Setting up the database, confirming connection, and listing tables.
"""

import sqlite3
import pandas as pd
import database_query_functions as dbq

# Direct access to the path since you'll use it constantly
database_path = dbq.database_path

class DatabaseModifier:
    def __init__(self):
        self.conn = sqlite3.connect(database_path)
        dbq._register_regex(self.conn)  # Reuse regex functionality

