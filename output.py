import json
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import date
from FbProphet_import_modules.db_connector import DB
import matplotlib.pyplot as plt

class Output:
    """ Create and write output predictions. Compute training and test prediction errors. """

    def __init__(self, model, d_train, d_test, df):
        
        with open('/dbfs/FileStore/tables/config.json') as f:
            data = json.load(f)
        
        
        self.model = model
        self.d_train = d_train
        self.d_test = d_test
        self.df = df
        
        # All the data below is fetched from the configuration file
        self.weeks_to_predict = int(data["weeks_to_predict"])
        self.db_type = data["db_type"]
        self.company = data["company"]


    def output(self):
        
        self.make_prediction()
        error = self.compute_error()
        print('Training MAPE', error['train']['MAPE'])
        print('Test MAPE', error['test']['MAPE'])
        print('Training MAE', error['train']['MAE'])
        print('Test MAE', error['test']['MAE'])
        #Logging sizes
        print('Training data size', self.d_train.shape[0])
        print('Test data size', self.d_test.shape[0])
        fcst_output = self.prepare_output()
        
        return fcst_output, self.db_type, self.model, self.pred_trans, self.df

    def make_prediction(self):
        
        
        # make basis and predicted
        domain_pred = self.model.make_future_dataframe(periods=self.d_test.shape[0] + self.weeks_to_predict
                                                       , freq='W', include_history=False)
        # Adding 1 day because stupid prophet uses sunday as first day.
        domain_pred['ds'] = domain_pred['ds'] + timedelta(days=1)
    
        domain_pred['type'] = np.where(domain_pred.index <= self.d_test.shape[0], 'test', 'fcst')
        print(domain_pred)

        domain_history = self.d_train[['ds', 'type']].copy()
        domain_combined = pd.concat([domain_history, domain_pred])
    
        pred = self.model.predict(domain_combined)
    
        # Transforming back
        pred_trans = pd.DataFrame(data={
            'ds': pred['ds'],
            'yhat_lower': np.exp(pred['yhat_lower']),
            'yhat_upper': np.exp(pred['yhat_upper']),
            'yhat': np.exp(pred['yhat'])
        })
    
        self.pred_trans = pd.merge(pred_trans, domain_combined, how='inner', on='ds')
    
        
    def compute_error(self):
        
        """ Calculate MAPE and MAE of the forecast
        
        """
        
        temp = self.df.copy()
        check = pd.merge(
            temp[['ds', 'y', 'type']],
            self.pred_trans[['ds', 'yhat']],
            on = 'ds'
        )
        check['e'] = check['y'] - check['yhat']
        check['p'] = 100 * check['e'] / check['y']
        return {
            'train': {
                'MAPE': np.mean(np.abs(check[check['type'] == 'train']['p'])),
                'MAE': np.mean(np.abs(check[check['type'] == 'train']['e']))
            },
            'test': {
                'MAPE': np.mean(np.abs(check[check['type'] == 'test']['p'])),
                'MAE': np.mean(np.abs(check[check['type'] == 'test']['e']))
            }
        }
            
    def prepare_output(self):
        
        """ Get the output data on the correct format
        
        """
        
        self.fcst_output = self.pred_trans[self.pred_trans['type'] == 'fcst'][['ds', 'yhat']].copy()
        self.fcst_output['year'] = self.fcst_output['ds'].apply(lambda x: x.isocalendar()[0])
        self.fcst_output['week'] = self.fcst_output['ds'].apply(lambda x: x.isocalendar()[1])
        
        current_date = date.today().isocalendar()
        
        self.fcst_output['current_week'] = current_date[1]
        self.fcst_output['current_year'] = current_date[0]
        
        self.fcst_output['forecast'] = np.round(self.fcst_output['yhat']).astype(int)
        self.fcst_output['run_timestamp'] = pd.Timestamp.now()
        
        if self.company == 'Godtlevert':
            self.fcst_output['company_id'] = "'09ECD4F0-AE58-4539-8E8F-9275B1859A19'"
        elif self.company == 'Adams Matkasse':
             self.fcst_output['company_id'] = "'8A613C15-35E4-471F-91CC-972F933331D7'"
        elif self.company == 'Linas Matkasse':
            self.fcst_output['company_id'] = "'8A613C15-35E4-471F-91CC-972F933331D7'"
        elif self.company == 'Proviant':
            self.fcst_output['company_id'] = "'E356EF97-15EE-44D4-9991-F50F1424949C'"
        else:
            print('Choose an existing company!')
        
        return self.fcst_output
        
            
            

       
