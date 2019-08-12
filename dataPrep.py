import pandas as pd
import numpy as np
import json
from FbProphet_import_modules.db_connector import DB

class DataPrep:
    """ Takes all actions performed on Data """

    def __init__(self):
        
        with open('/dbfs/FileStore/tables/config.json') as f:
            data = json.load(f)
        
        # All the data below is fetched from the configuration file
        self.split = float(data["train_test_split"])
        self.company = data["company"]
        self.db_type = data["db_type"]
    
    def data_prep(self):
        
        self.data_load()
        train, test = self.train_test_split()
        
        return train, test, self.d
    
    def data_load(self):
    
        """ Load data into dataframe using SQL query
    
        """
        
        db = DB(db_type=self.db_type)
        
        query = "(SELECT o.delivery_year,o.delivery_week,CAST([crm].[find_first_day_of_week](o.delivery_year, o.delivery_week) as varchar) as [ds], COUNT(o.delivery_year) as [y] FROM mb.orders o WHERE o.company_id = {0} AND o.delivery_year >= 2015 GROUP BY o.delivery_year,o.delivery_week) orders_alias"
        
        if self.company == 'Godtlevert':
            self.company_id = "'09ECD4F0-AE58-4539-8E8F-9275B1859A19'"
        elif self.company == 'Adams Matkasse':
             self.company_id = "'8A613C15-35E4-471F-91CC-972F933331D7'"
        elif self.company == 'Linas Matkasse':
            self.company_id = "'8A613C15-35E4-471F-91CC-972F933331D7'"
        elif self.company == 'Proviant':
            self.company_id = "'E356EF97-15EE-44D4-9991-F50F1424949C'"
        else:
            print('Choose an existing company!')
        
        query = query.format(self.company_id)

        self.d = spark.read.jdbc(url=db.jdbcUrl, table=query, properties=db.connectionProperties)
        self.d.orderBy("delivery_year","delivery_week")
        self.d['ds'] = pd.to_datetime(self.d['ds'])


    def make_model_dataframe(self, d):
    
        """ Returns the model dataframe, organized and with essential columns only
    
        """
        
        return pd.DataFrame(data={'ds': d['ds'],
                                  'y': np.log(d['y']),
                                  'type': d['type']})


    def train_test_split(self):

        """ Divides data and returns training and test data
    
        """
        split_index = int(len(self.d) * self.split)
        self.d['type'] = np.where(self.d.index < split_index, 'train', 'test')
        
        self.d_model = self.make_model_dataframe(self.d)

        
        return self.d_model[self.d_model['type'] == 'train'],  self.d_model[self.d_model['type'] == 'test']
        
