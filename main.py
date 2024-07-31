
print("testing deploy")
import requests
from keboola.component.base import ComponentBase

class Component(ComponentBase):
    def __init__(self):
        super().__init__()
    def run(self):       
        params = self.configuration.parameters
        Token = params['BearerToken']
        file=params['FileName']
        dataset=params['Dataset']
        metadata=params['Metadata']
        
        url = "https://api.roe-ai.com/v1/datasets/files/upload/"
        with open(file, "rb") as File:
            payload = "-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\n{FileName}\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"dataset_id\"\r\n\r\n{dataset}\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n{metadata}\r\n-----011000010111000001101001--\r\n\r\n".format(FileName=File, metadata=metadata, dataset=dataset)
            headers = {
                "Authorization": "Bearer {Token}".format(Token=Token),
                "Content-Type": "multipart/form-data"
            }
            response = requests.request("POST", url, data=payload, headers=headers)

            print(response.text)

if __name__ == "__main__":
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    