import json
from azureml.core import Workspace
from azureml.core.authentication import ServicePrincipalAuthentication


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
        aad_client_definitions = data["active_directory_client"]
        
        self.mls_client = ServicePrincipalAuthentication(tenant_id=aad_client_definitions['tenant_id'],
                                                    service_principal_id=aad_client_definitions['client_id'],
                                                    service_principal_password=aad_client_definitions['client_secret'])
        

        
    def get_workspace(self):
    
        """ Gets workspace using specifications in definitions.json file
    
        """
        ws = Workspace.get(name = self.ml_services_name, subscription_id = self.subscription_id, 
                           resource_group = self.resource_group_name = self.mls_client)
        
        return ws
    

    
