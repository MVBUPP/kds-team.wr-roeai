
print("testing deploy")
import requests
import csv
import os
from keboola.component import CommonInterface
from keboola.component.base import ComponentBase
from requests_toolbelt.multipart.encoder import MultipartEncoder

class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):       
        params = self.configuration.parameters
        Token = params['BearerToken']
        TABLENAME=params["Dataset"]
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
               
               # remove extra column from column str
               payload={"query": "ALTER TABLE {tableName} DROP COLUMN buffercolumn;".format(tableName=TABLENAME)}
               response = requests.request("POST", url, json=payload, headers=headers)
          
               
                # Insert rest of data
               tableList=""
               # i=0
               add=True
               for lines in csvFile:
                         for elements in lines:
                               if elements.__contains__("'"):
                                   #   
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
               print(response.text)

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
    