
print("testing deploy")
import requests
from keboola.component.base import ComponentBase
from requests_toolbelt.multipart.encoder import MultipartEncoder

class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        
    def run(self):       
        params = self.configuration.parameters
        Token = params['BearerToken']
        file=params['FileName']
        dataset=params['Dataset']
        metadata=params['Metadata']
        content_type=params["Content_Type"]
        url = "https://api.roe-ai.com/v1/datasets/files/upload/"
         
        # dataset_id and metadata are OPTIONAL, so omit the payload if you don't need them
        payload = {
            'dataset_id': dataset,
            'metadata': metadata
        }

        # REQUIRED
        files=[
        ('file', (file, open(file,'rb'), content_type))
        ]

        headers = {
        "Authorization": "Bearer {Token}".format(Token=Token)
        }

        if(dataset or metadata):
             response = requests.request("POST", url, headers=headers, data=payload, files=files)
        else:
            response = requests.request("POST", url, headers=headers, files=files)

        print(response.text)
        
if __name__ == "__main__":
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    