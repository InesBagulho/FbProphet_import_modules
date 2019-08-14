
import pyodbc
import json
from pathlib import Path



class DB:
    """ Holds the connection to the Database"""

    def __init__(self, db_type, db_action):
        """ Connect to SQL SERVER db """

        if db_type == 'analytical':
            DB_SETTINGS = '/dbfs/FileStore/tables/dwh_settings.json'
        if db_type == 'core_prod':
            DB_SETTINGS = None
        if db_type == 'core_qa':
            DB_SETTINGS = None

        with open(DB_SETTINGS) as filename:
            conf = json.load(filename)
        if db_action == "read":
            conf_db = conf["read_db"]
        else if db_action == "write":
            conf_db = conf["write_db"]
            
        jdbcHostname = conf_db["host"]
        jdbcDatabase = conf_db["dbname"]
        jdbcPort = conf_db["port"]
        jdbcUsername = conf_db["user"]
        jdbcPassword = conf_db["passw"]
        jdbcDriver = conf_db["driver"]

        self.jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

        self.connectionProperties = {
            "user" : jdbcUsername,
            "password" : jdbcPassword,
            "driver" : jdbcDriver
        }
