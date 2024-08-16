
print("testing deploy")
import requests
import os
import csv
from keboola.component.base import ComponentBase
from requests_toolbelt.multipart.encoder import MultipartEncoder

class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):       
        params = self.configuration.parameters
        Token = params['BearerToken']
        # file=params['FileName']
        TABLENAME=params["TableName"]
        # COLUMNTUPLE=params["Columns"]
        # url = "https://api.roe-ai.com/v1/datasets/files/upload/"
        url = "https://api.roe-ai.com/v1/database/query/"

        # payload = {"query": "CREATE TABLE {tableName} (x String, y String, z String) ENGINE = Memory AS SELECT 1;".format(tableName=TABLENAME)}
        headers = {
        "Authorization": "Bearer {Token}".format(Token=Token)
        }
        # response = requests.request("POST", url, json=payload, headers=headers)
        # print(response.text)

        with open('in/test1.csv', mode ='r')as file:
            csvFile = csv.reader(file)
            line1 = next(csvFile)
            for i in range(len(line1)):
                 if(i==0):
                      line1[0]=line1[0][3:]
                 line1[i]=line1[i]+" String"
            colstr="("
            for item in tuple(line1):
                 colstr=colstr+item
                 colstr=colstr+", "
            colstr=colstr+"buffercolumn String)"    
            print(colstr)
            payload = {"query": "CREATE TABLE {tableName} {columns} ENGINE = Memory AS SELECT 1;".format(tableName=TABLENAME, columns=colstr)}
            # payload = {"query": "CREATE TABLE {tableName} ENGINE = MergeTree;".format(tableName=TABLENAME)}
            response = requests.request("POST", url, json=payload, headers=headers)
            print(response.text)
            
            payload={"query": "ALTER TABLE {tableName} DROP COLUMN buffercolumn;".format(tableName=TABLENAME)}
            response = requests.request("POST", url, json=payload, headers=headers)
            print(response.text)
            
            for lines in csvFile:
                    payload = {"query": "INSERT INTO {tableName} VALUES {line}".format(tableName=TABLENAME,line=tuple(lines))}
                    response = requests.request("POST", url, json=payload, headers=headers)
                    print(response.text)
        # dataset_id and metadata are OPTIONAL, so omit the payload if you don't need them
        # payload = {
        #     'dataset_id': dataset,
        #     'metadata': metadata
        # }
        
        # REQUIRED
        # files=[]
        # for filename in os.listdir("in/files"):
        #     f = os.path.join("in/files", filename)
        #     # checking if it is a file
        #     if os.path.isfile(f):

        #         files.append(('file', (filename, open(f,'rb'), content_type)))
        # # files=[
        # # ('file', (file, open(("in/tables/{file}").format(file=file),'rb'), content_type))
        # # ]

        # headers = {
        # "Authorization": "Bearer {Token}".format(Token=Token)
        # }
        # for file1 in files:
        #     file1=[file1]
        #     if(dataset or metadata):
        #         response = requests.request("POST", url, headers=headers, data=payload, files=file1)
        #     else:
        #         response = requests.request("POST", url, headers=headers, files=file1)
        #         print(file1)

        #     print(response.text)
        
if __name__ == "__main__":
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    