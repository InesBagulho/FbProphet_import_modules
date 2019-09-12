import pandas as pd
import numpy as np
import json
from db_connector import DB

class DataPrep:
    """ Takes all actions performed on Data """

    def __init__(self, split, d_pandas):
        
        self.split = split
        self.d_pandas = d_pandas
    
    def data_prep(self):
        
        train, test = self.train_test_split()
        
        return train, test

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
        
