import pandas as pd
import numpy as np
import json
from db_connector import DB
import spark

class DataPrep:
    """ Takes all actions performed on Data """

    def __init__(self, split, db_type, company):
        
        self.split = split
        self.company_id = company
        self.db_type = db_type
    
    def data_prep(self):
        
        self.data_load()
        train, test = self.train_test_split()
        
        return train, test, self.d_pandas
    
    def data_load(self):
    
        """ Load data into dataframe using SQL query
    
        """
        
        db = DB(db_type=self.db_type, db_action="read")
        
        query = """(SELECT delivery_year, delivery_week, ds, y
                    FROM ml.input_forecast_orders 
                    WHERE company_id = '{}') orders_alias"""
        
        query = query.format(self.company_id)

        d = spark.read.option("numPartitions", 50).jdbc(url=db.jdbcUrl, table=query, properties=db.connectionProperties)
        d.orderBy("delivery_year", "delivery_week")
        self.d_pandas = d.toPandas()
        self.d_pandas['ds'] = pd.to_datetime(d_pandas['ds']) 


    def make_model_dataframe(self, d):
    
        """ Returns the model dataframe, organized and with essential columns only
    
        """
        
        return pd.DataFrame(data={'ds': d['ds'],
                                  'y': np.log(d['y']),
                                  'type': d['type'],
                                  'oddweek': np.where((d['delivery_week'] % 2) == 0, 0, 1)})


    def train_test_split(self):

        """ Divides data and returns training and test data
    
        """
        split_index = int(len(self.d_pandas) * self.split)
        self.d_pandas['type'] = np.where(self.d_pandas.index < split_index, 'train', 'test')
        
        self.d_model = self.make_model_dataframe(self.d_pandas)

        
        return self.d_model[self.d_model['type'] == 'train'],  self.d_model[self.d_model['type'] == 'test']
        
