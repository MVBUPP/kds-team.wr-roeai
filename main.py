
print("testing deploy")
import requests
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
# Load the Component library to process the config file
from keboola.component import CommonInterface

class Component(ComponentBase):
    def __init__(self):
        super().__init__()
    def run(self):       
        params = configuration.parameters
        TOKEN = params['BearerToken']

        url = "https://api.roe-ai.com/v1/datasets/files/upload/"

        payload = "-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\n<string>\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"dataset_id\"\r\n\r\n<string>\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n<any>\r\n-----011000010111000001101001--\r\n\r\n"
        headers = {
            "Authorization": "Bearer {TOKEN}",
            "Content-Type": "multipart/form-data"
        }

        response = requests.request("POST", url, data=payload, headers=headers)

        print(response.text)

if __name__ == "__main__":
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    