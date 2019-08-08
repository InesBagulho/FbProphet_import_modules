
import pyodbc
import json
from pathlib import Path



class DB:
    """ Holds the connection to the Database"""

    def __init__(self, db_type):
        """ Connect to SQL SERVER db """

        if db_type == 'analytical':
            DB_SETTINGS = '/dbfs/FileStore/tables/dwh_settings.json'
        if db_type == 'core_prod':
            DB_SETTINGS = None
        if db_type == 'core_qa':
            DB_SETTINGS = None

        with open(DB_SETTINGS) as filename:
            conf = json.load(filename)

        conn_str = 'DATABASE={{{}}};UID={{{}}};SERVER={{{}}};PORT={{{}}};PWD={{{}}}'.format(
            conf['dbname'], conf['user'], conf['host'], conf['port'],
            conf['passw']
        )

        self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'+conn_str)
        self.cur = self.conn.cursor()

    def close(self):
        """ Closes the connection """
        self.cur.close()
        self.conn.close()

    def commit(self):
        """ Commits a change in the db """
        self.conn.commit()
