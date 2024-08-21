
print("testing deploy")
import logging
import requests
import csv
import os
from kbcstorage.client import Client
from keboola.component import CommonInterface
from keboola.component.base import ComponentBase
from requests_toolbelt.multipart.encoder import MultipartEncoder
from configuration import Configuration
# from client.googleai_client import GoogleAIClient


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    def run(self):       
        params = self.configuration.parameters
        self._init_configuration()
        Token = self._configuration.credentials.pswd_api_token
     #Token = self.configuration.image_parameters.get(KEY_API_TOKEN)
     #    credentials=params["credentials"]
     #    Token= credentials["api_token"]
     
        TABLENAME=params["dataset"]
        url = "https://api.roe-ai.com/v1/database/query/"
        headers = {
        "Authorization": "Bearer {Token}".format(Token=Token)
        }
     #    UNCOMMENT
        ci=CommonInterface()
        tables=ci.configuration.tables_input_mapping
        ###TEMP FILE ACCESS###
     #    path = "in/tables/"
     #    dir_list = os.listdir(path)
     #    fileName=dir_list[0]

        for table in tables:
          # inName=table.destination
          table_def=ci.get_input_table_definition_by_name(table.destination)
     #UNCOMMENT TO HERE
        # open csv file to parse column str for query
     #    with open('in/tables/{filename}'.format(filename=fileName), mode ='r')as file:
        with open(table_def.full_path, mode ='r')as file: #UNCOMMENT 
     #    with open('in/tables/{filename}'.format(filename=fileName), mode ='r')as file:
               csvFile = csv.reader(file)
               line1 = next(csvFile)
               for i in range(len(line1)):
                    line1[i]=line1[i]+" String"
               colstr="("
               for item in tuple(line1):
                    colstr=colstr+item
                    colstr=colstr+", "
               colstr=colstr+"buffercolumn String)"    
               
               #create table
               payload = {"query": "CREATE TABLE {tableName} {columns} ENGINE = Memory AS SELECT 1;".format(tableName=TABLENAME, columns=colstr)}
               response = requests.request("POST", url, json=payload, headers=headers)
               if (response.status_code == 200):
                     logging.info("Request to create table in RoeAI succeeded.")
               elif (response.status_code == 404):
                     logging.info("Request to create table in RoeAI failed.")
               
               # remove extra column from column str
               payload={"query": "ALTER TABLE {tableName} DROP COLUMN buffercolumn;".format(tableName=TABLENAME)}
               response = requests.request("POST", url, json=payload, headers=headers)
               if (response.status_code == 200):
                     logging.info("Request to drop column in RoeAI succeeded.")
               elif (response.status_code == 404):
                     logging.info("Request to drop column in RoeAI failed.") 

                # Insert rest of data
               tableList=""
               # i=0
               add=True
               logging.info("Table is being loaded into RoeAI...")
               for lines in csvFile:
                         for elements in lines:
                               if elements.__contains__("'"):
                                   logging.info("Entry contains unescaped ' in column.")   
                                   add=False
                         if add:
                              tableList+="{newLine}, ".format(newLine=tuple(lines))
                         else:
                              add=True    
               #           i+=1
               #           if i==500:
               #                tableList=tableList[:-2]
               #                payload = {"query": "INSERT INTO {tableName} VALUES {line}".format(tableName=TABLENAME,line=tableList)}
               #                response = requests.request("POST", url, json=payload, headers=headers)
               #                print(response.text)
               #                tableList=""
               #                i=0
               tableList=tableList[:-2]
               payload = {"query": "INSERT INTO {tableName} VALUES {line}".format(tableName=TABLENAME,line=tableList)}
               response = requests.request("POST", url, json=payload, headers=headers)
               if (response.status_code == 200):
                     logging.info("Request to insert table into RoeAI succeeded.")
               elif (response.status_code == 404):
                     logging.info("Request to insert table into RoeAI failed.")

        ######Potential Single File implementation######
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
    