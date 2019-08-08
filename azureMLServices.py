import json
from azureml.core import Workspace

class AML:
    """ Takes all actions associated with Azure ML Services """

    def __init__(self):
        
        with open('/dbfs/FileStore/tables/definitions.json') as f:
            data = json.load(f)
        
        # All the data below is fetched from the configuration file
        self.subscription_id = data["subscription_id"]
        self.resource_group_name = data["resource_group_name"]
        self.ml_services = data["ml_services"]
        self.ml_services_name = self.ml_services["ml_services_name"]
        self.region = self.ml_services["region"]
        self.compute_target = data["aml_compute_target"]

        
    def get_workspace(self):
    
        """ Gets workspace using specifications in definitions.json file
    
        """
        ws = Workspace.get(name = self.ml_services_name, subscription_id = self.subscription_id, 
                           resource_group = self.resource_group_name)
        
        return ws
    

    
