import pandas as pd
import json
import holidays
import os
import pickle
from fbprophet import Prophet
from datetime import date
from datetime import datetime
from azureml.core.model import Model
from FbProphet_import_modules.azureMLServices import AML

class FBModel:
    """ Takes all actions performed to train, save and register the model """

    def __init__(self, train):
        
        with open('/dbfs/FileStore/tables/config.json') as f:
            data = json.load(f)
        
        self.data_train = train
        
        # All the data below is fetched from the configuration file
        self.country = data["country"]
        self.model_path = data["model_path"]
        self.model_file = data["model_file"]
        self.model_name = data["model_name"]
        
        
    def fbmodel(self):
        
        model = self.model_fit()
        self.save_and_register_model()
        
        return model
        
        
    def model_fit(self):
    
        """ Fit fbprophet model to the training data
    
        """
        
        holidays_df = self.get_holidays()
        self.m = Prophet(holidays = holidays_df)
        self.m.fit(self.data_train)
        
        return self.m
    

    def get_holidays(self):
        
        """ 
            Holiday handling. Sets holiday effect to the interval of plus one 
            day minus the days before that in the week. And adds a specific
            effect to the fellesferie mondays
            """
        
        if self.country == "NO":
            hdays = holidays.Norway(years = range(2015,2021), include_sundays=False)
        if self.country == "SE":
            hdays = holidays.Sweden(years = range(2015,2021), include_sundays=False)
        hdays_array = []
        
        for dates, name in hdays.items():
            hdays_array.append([dates, name])   
            
        hdays_df = pd.DataFrame(hdays_array, columns=['ds', 'holiday'])
        hdays_df['lower_window'] = - hdays_df['ds'].apply(lambda x: x.weekday())
        hdays_df['upper_window'] = 2
    
        # Get fellesferie
        fellesferie_mondays = pd.DataFrame(columns=['ds', 'holiday'])
        i = 1
        for year in hdays.years:
            print(date(year, 7, 1))
            all_days = pd.date_range(start=date(year, 7, 1), end=date(year, 7, 31), freq='W-MON')[-3:]
            print(all_days)
            all_days_df = pd.DataFrame({'ds': all_days})
            all_days_df['holiday'] = 'felles_'+(all_days_df.index+1).astype(str)
            fellesferie_mondays = pd.concat([fellesferie_mondays, all_days_df])
    
        fellesferie_mondays['lower_window'] = 0
        fellesferie_mondays['upper_window'] = 6
        
        hdays_df = pd.concat([hdays_df, fellesferie_mondays])
        
        return hdays_df
    
    
    def save_and_register_model(self):
        
        """ 
            Saves the model locally and register's it in Azure ML Services
            
        """
        
        os.makedirs(self.model_path, exist_ok=True)
        pickle.dump(self.m, open(self.model_path + self.model_file,"wb"))
        
        timenow = datetime.now().strftime('%m-%d-%Y-%H-%M')
        
        Model.register(model_path = self.model_path + self.model_file, # this points to a local file
                       model_name = self.model_name, # this is the name the model is registered as, am using same name for both path and name.                 
                       description = "Trained model using fbprophet at " + timenow,
                       workspace = AML().get_workspace())
