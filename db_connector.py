
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
        
        jdbcHostname = conf["host"]
        jdbcDatabase = conf["dbname"]
        jdbcPort = conf["port"]
        jdbcUsername = conf["user"]
        jdbcPassword = conf["passw"]
        jdbcDriver = conf["driver"]

        self.jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

        self.connectionProperties = {
            "user" : jdbcUsername,
            "password" : jdbcPassword,
            "driver" : jdbcDriver
        }
