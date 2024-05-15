from distutils.util import strtobool
import os
import requests
import json
import uuid
import time
import configs
# Generate a unique client ID using UUID
identity_client_id = "f827b4b5-3d86-1412-09fb-22d8577ded8f"
nifi_url = "https://sds-nifi-headless.qa.svc.cluster.local:8443/nifi-api"
nifi_registry_url = 'https://sds3-qasprint.dev.prevalent.ai'
os.environ.get('IS_NIFI_SSL_VERIFY_ENABLED')
os.environ.get('IS_NIFI_AUTH_ENABLED')
os.environ.get('NIFI_CERT_PATH')
os.environ.get('NIFI_KEY_PATH')

def get_env_variable(name, default=None):
    """
    Util method to load environment variable for SECRET keys in settings.
    :param name:
    :return:
    """
    try:
        return os.environ.get(name, default) if (default is not None) else os.environ[name]
    except KeyError:
        raise 'Environment variable not found.'
    
def nifi_rest_api_prerequisites():
    cert_details = None
    verify = bool(strtobool(get_env_variable('IS_NIFI_SSL_VERIFY_ENABLED', 'False')))
    if bool(strtobool(get_env_variable('IS_NIFI_AUTH_ENABLED', 'True'))):
        cert_details = (os.getenv("NIFI_CERT_PATH", None), os.getenv("NIFI_KEY_PATH", None))
    return [verify, cert_details]

def get_base_canvas_id():
    url= f"{nifi_url}/process-groups/root"
    response_json = requests.get(url,cert=cert_details, verify=verify).json()
    return response_json['id']

#client_id
# def get_nifi_client_id():
#     url= f"{nifi_url}/flow/client-id"
#     response_text = requests.get(url,cert=cert_details, verify=verify).text
#     return response_text

#To enable all the controller services
def enable_pg_contoller_serverices(state):
        url=f'{nifi_url}/flow/process-groups/{pg_id}'
        payload=configs.enable_pg_contoller_serverices(pg_id,state)
        # Send a PUT request with the payload
        response_json=requests.put(url,json=payload,cert=cert_details, verify=verify).json()

#To set the variables
def set_variables_pg(source_name,srdm_table_name,vendor,bucket_name,container,input_directory,source_path):
    url=f'{nifi_url}/process-groups/{pg_id}/variable-registry'
    response_json=requests.get(url,cert=cert_details, verify=verify).json()
    revision=response_json["processGroupRevision"]["version"]

    url= f'{nifi_url}/process-groups/{pg_id}/variable-registry/update-requests'  
    headers = {"Content-Type": "application/json"}
    payload =configs.set_variables_pg(revision,pg_id,source_name,srdm_table_name,vendor,bucket_name,container,input_directory,source_path)
    response = requests.post(url, headers=headers, json=payload, cert=cert_details, verify=verify)
    if response.status_code == 202:
            print("Successfully updated variables of processor group")
            return response.json()
    else:
            print(f"Failed to update variables of processor group. Status code: {response.status_code}")
            return None

#To get the AWS controller service ID
def get_creds_controller_service_id(id):
    url= f'{nifi_url}/flow/process-groups/{id}/controller-services'
    response_json=requests.get(url,cert=cert_details, verify=verify).json()
    for service in response_json["controllerServices"]:
        if service['parentGroupId'] == template_id:
            creds_controller_service_id=service['component']['id']
            break
    return creds_controller_service_id

#to get process ids of processors inside the Processor Groups
def get_processor_id(id,name):
    url= f"{nifi_url}/process-groups/{id}/processors"
    response_json = requests.get(url,cert=cert_details, verify=verify).json()
    for processor in response_json['processors']:
            if processor['component']['name'] == name:
                return (processor['id'], processor['revision']['version'])

#set properties of processors
def set_azure_processor_props(id,name,service_id,version):
    url= f"{nifi_url}/processors/{id}"
    headers = {'Content-Type': 'application/json'}
    payload=configs.set_azure_processor_props(id,name,service_id,version)
    response = requests.put(url, headers=headers, json=payload,cert=cert_details, verify=verify)
    if response.status_code == 200:
            print("Successfully updated properties of processor")
            return response.json()
    else:
            print(f"Failed to update properties of processor. Status code: {response.status_code}")
            return None

def set_aws_processor_props(id,name,service_id,version):
    url= f"{nifi_url}/processors/{id}"
    headers = {'Content-Type': 'application/json'}
    payload=configs.set_aws_processor_props(id,name,service_id,version)
    response = requests.put(url, headers=headers, json=payload,cert=cert_details, verify=verify)
    if response.status_code == 200:
            print("Successfully updated properties of processor")
            return response.json()
    else:
            print(f"Failed to update properties of processor. Status code: {response.status_code}")
            return None

def set_variables_publisher(output_bucket,output_object_path):
    nifi_api_url= f'{nifi_url}/process-groups/{publisher_id}/variable-registry/update-requests'  
    headers = {"Content-Type": "application/json"}
    payload =configs.set_variables_publisher(publisher_ver,publisher_id,output_bucket,output_object_path)       
    response = requests.post(nifi_api_url, headers=headers, data=json.dumps(payload),cert=cert_details, verify=verify)
    if response.status_code == 202:
        print("Updated variables of Publisher processor group")
    else:
        print(f'Failed to update variables of Publisher processor group: {response.status_code}, Response: {response.text}')

def pg_status(state):
    url= f"{nifi_url}/flow/process-groups/{template_id}"
    headers = {'Content-Type': 'application/json'}
    payload=configs.pg_pg(template_id,state)
    response = requests.put(url, headers=headers, json=payload,cert=cert_details, verify=verify)
    if response.status_code == 200:
            print(f"Processor groups Successfully {state} ")
            return response.json()
    else:
            print(f"Failed to Start processor groups. Status code: {response.status_code}")
            return None

def delete_connection(source_processor_name,id):
    url= f"{nifi_url}/flow/process-groups/{id}"
    response = requests.get(url,cert=cert_details, verify=verify)
    response_json = response.json()

    for connection in response_json['processGroupFlow']['flow']['connections']:
        if connection['component']['source']['name'] == source_processor_name:
            connection_id = connection['component']['id']
            break
    print("Connection ID",connection_id)
    url= f'{nifi_url}/connections/{connection_id}'
    response_json = requests.get(url,cert=cert_details, verify=verify).json()
    connection_vers=response_json["revision"]["version"]
    url= f'{nifi_url}/connections/{connection_id}'
    
    params=configs.delete_connection(connection_vers)
    response = requests.delete(url,params=params,cert=cert_details, verify=verify)
    if response.status_code == 200:
         print("Deleted connection........Successfully.............")
    else:
         print(f'Failed to delete connection: {response.status_code}, Response: {response.text}')

def add_splitjson(x,y):
    nifi_api_url= f'{nifi_url}/process-groups/{pg_id}/processors'  
    headers = {"Content-Type": "application/json"}
    payload =configs.add_splitjson(x,y)     
    response = requests.post(nifi_api_url, headers=headers, data=json.dumps(payload),cert=cert_details, verify=verify)
    if response.status_code == 201:
        print("Pulled SplitJSON........Successfully.............")
    else:
        print(f'Failed to pull SplitJSON processor: {response.status_code}, Response: {response.text}')
    response_json=response.json()
    splitjson_id=response_json['id']
    return splitjson_id

def clearstate(first_processor_id):
    url= f"{nifi_url}/processors/{first_processor_id}/state/clear-requests"
    headers = {"Content-Type": "application/json"}
    payload = {}         
    response = requests.post(url, headers=headers, data=payload,cert=cert_details, verify=verify)
    if response.status_code == 200:
        print("Cleared the state ........Successfully.............")
    else:
        print(f'Failed to clear the state: {response.status_code}, Response: {response.text}')

def create_connection(source_id,source_gp_id,s_type,destination_id,dest_gp_id,d_type,relationship=None):
    if d_type=='FUNNEL':
        url= f'{nifi_url}/process-groups/{dest_gp_id}/connections'  
    else:
        url= f'{nifi_url}/process-groups/{source_gp_id}/connections'    
    headers = {"Content-Type": "application/json"}
    payload =configs.create_connection(source_id,source_gp_id,s_type,destination_id,dest_gp_id,d_type,relationship)
    response = requests.post(url, headers=headers, data=json.dumps(payload),cert=cert_details, verify=verify)
    if response.status_code == 201:
        print("Created Connection ........Successfully.............")
    else:
        print(f'Failed to create connection: {response.status_code}, Response: {response.text}')

def copy_pg(parent_pg_id,pg_id,name_copy_pg,x,y):
    url= f"{nifi_url}/snippets"
    headers = {'Content-Type': 'application/json'}
    payload=configs.copy_pg(parent_pg_id,pg_id)
    response_json = requests.post(url, headers=headers, json=payload,cert=cert_details, verify=verify).json()
    snippet_id=response_json["snippet"]["id"]
    
    #pasting the copy of the singleline processor
    url= f"{nifi_url}/process-groups/{parent_pg_id}/snippet-instance"
    payload=configs.paste_pg(snippet_id,x,y)
    response_json = requests.post(url, headers=headers, json=payload,cert=cert_details, verify=verify).json()
    process_group = response_json["flow"]["processGroups"][0]
    pg_id = process_group["id"]
    revision_version = process_group["revision"]["version"]
    #renaming the processor
    url= f"{nifi_url}/process-groups/{pg_id}"
    payload=configs.rename_pg(name_copy_pg,pg_id,revision_version)
    response = requests.put(url, headers=headers, json=payload,cert=cert_details, verify=verify)
    if response.status_code == 200:
            print("Successfully created singleline processor group")
            return pg_id
    else:
            print(f"Failed to create singleline processor group. Status code: {response.status_code}")
            return None

def get_pg_id(parent_pg_id,pg_name):
    url= f"{nifi_url}/flow/process-groups/{parent_pg_id}"
    response_json = requests.get(url,cert=cert_details, verify=verify).json()   
    for process_group in response_json['processGroupFlow']['flow']['processGroups']:
        if process_group['component']['name'] == pg_name:
            component_id = process_group['component']['id']
            version = process_group['revision']['version']
            break
    return component_id, version

def get_output_port_id(pg_id,output_port_name):
    url= f"{nifi_url}/flow/process-groups/{pg_id}"
    response = requests.get(url,cert=cert_details, verify=verify)
    response_json = response.json()
    for process_group in response_json['processGroupFlow']['flow']['outputPorts']:
        if process_group['component']['name'] == output_port_name:
            component_id = process_group['component']['id']
            break
    return component_id

def get_funnel_id(pg_id,destination_name):
    url= f"{nifi_url}/flow/process-groups/{pg_id}"
    response = requests.get(url,cert=cert_details, verify=verify)
    response_json = response.json()
    for connection in response_json['processGroupFlow']['flow']['connections']:
        if connection['component']['destination']['name'] == destination_name:
            funnel_id = connection['component']['source']['id']
            break
    return funnel_id

def set_publisher_processor_props(publisher_processor_id,publisher_processor_name,region,publisher_processor_version):
    url= f'{nifi_url}/processors/{publisher_processor_id}'  
    headers = {"Content-Type": "application/json"}
    payload =configs.set_publisher_processor_props(publisher_processor_id,publisher_processor_name,region,publisher_processor_version)         
    response = requests.put(url, headers=headers, data=json.dumps(payload),cert=cert_details, verify=verify)
    if response.status_code == 200:
        print("Updated the properties of the processor ........Successfully.............")
    else:
        print(f'Failed to update the properties of the processor: {response.status_code}, Response: {response.text}')

def set_azure_controller_props(creds_controller_service_id,storage_name,identity_client_id):
    url= f'{nifi_url}/controller-services/{creds_controller_service_id}'  
    headers = {"Content-Type": "application/json"}
    payload =configs.set_azure_controller_props(creds_controller_service_id,storage_name,identity_client_id)         
    response = requests.put(url, headers=headers, data=json.dumps(payload),cert=cert_details, verify=verify)
    if response.status_code == 200:
        print("Updated the properties of the controller services ........Successfully.............")
    else:
        print(f'Failed to update the properties of the controller services: {response.status_code}, Response: {response.text}')

def add_replace_text_processor(id):
    url= f'{nifi_url}/process-groups/{id}/processors'  
    headers = {"Content-Type": "application/json"}
    payload =configs.add_replace_text_processor()     
    response = requests.post(url, headers=headers, data=json.dumps(payload),cert=cert_details, verify=verify)
    if response.status_code == 201:
        print("Pulled Replace Text........Successfully.............")
    else:
        print(f'Failed to pull Replace Text processor: {response.status_code}, Response: {response.text}')
    response_json=response.json()
    replace_text_id=response_json['id']
    return replace_text_id

def update_replace_text_props(processor_id,version):
    url= f'{nifi_url}/processors/{processor_id}'  
    headers = {"Content-Type": "application/json"}
    payload =configs.update_replace_text_props(processor_id,version)         
    response = requests.put(url, headers=headers, data=json.dumps(payload),cert=cert_details, verify=verify)
    if response.status_code == 200:
        print("Updated the properties of the processor ........Successfully.............")
    else:
        print(f'Failed to update the properties of the processor: {response.status_code}, Response: {response.text}')



verify, cert_details = nifi_rest_api_prerequisites()

bucket_name="pe-sds3-qasprint-datalake"
container="sds-qasprint-datalake-ttv"
input_directory="qa-test-data/raw_edm/defender_device_list/day1/"
source_name="defender_device_list"
source_path="qa-test-data/raw_edm/defender_device_list/"
srdm_table_name="microsoft_azure__defender_device_list"
vendor="microsoft_azure"

#return NIFI base canvas id..........................
base_canvas_id= get_base_canvas_id()
print("Base canvas ID: ",base_canvas_id)

# to get the id of SDS Autoparser Ingestion Template
template_id,template_ver=get_pg_id(base_canvas_id,'SDS Autoparser Ingestion Template')
print("Template ID:", template_id)

# to get the id of Receiver
receiver_id,receiver_ver=get_pg_id(template_id,'Reciever')
print("Receiver ID:", receiver_id)

# to get the id of Publisher
publisher_id,publisher_ver=get_pg_id(template_id,'Publisher')
print("Publisher ID:", publisher_id)
print("Publisher VErsion:", publisher_ver)

# to get the id of Batch
batch_id,batch_ver=get_pg_id(receiver_id,'Batch')
print("batch ID:", batch_id)


ch='aws'
if ch=='azure':
        # to get the id of S3
        Azure_blob_storage_id,Azure_blob_storage_ver=get_pg_id(batch_id,'Azure-blob-storage')
        print("Azure_blob_storage ID:", Azure_blob_storage_id)

        # to get the id of Singleline
        singleline_id,singleline_ver=get_pg_id(Azure_blob_storage_id,'ds4_custom_single_line')
        print("Singleline ID:", singleline_id)

        # to get the id of Multiline
        multiline_id,multiline_ver=get_pg_id(Azure_blob_storage_id,'ds5_custom_multiline')
        print("Multiline ID:", multiline_id)

        #creating a copy of singleline processor group
        x=1568
        y=1984
        pg_id=copy_pg(Azure_blob_storage_id,singleline_id,"Defender",x,y)
        #multi_pg_id=copy_pg(Azure_blob_storage_id,multiline_id,client_id,"AD")

        new_source1_out_id=get_output_port_id(pg_id,'new_source1-out')
        print("New source1 out ID:", new_source1_out_id)

        funnel_id=get_funnel_id(Azure_blob_storage_id,'azure-out')
        print("Funnel ID:", funnel_id)

        #to create connection between defender Processor_group and Funnel
        response_connection_funnel_pg=create_connection(new_source1_out_id,pg_id,"OUTPUT_PORT",funnel_id,Azure_blob_storage_id,"FUNNEL")
        print(response_connection_funnel_pg)

        azure_controller_service_id=get_creds_controller_service_id(pg_id)
        print(azure_controller_service_id)

        set_azure_controller_props(azure_controller_service_id,"paisdsqasprintincesa",identity_client_id)
        enable_pg_contoller_serverices("ENABLED")

        response_set_variables_pg=set_variables_pg(source_name,srdm_table_name,vendor,bucket_name,container,input_directory,source_path)
        print(response_set_variables_pg)



        #to get process ids of lists3
        ListAzureDataLakeStorage_id, ListAzureDataLakeStorage_version=get_processor_id(pg_id,"ListAzureDataLakeStorage")
        print(ListAzureDataLakeStorage_id,ListAzureDataLakeStorage_version)

        #to get process ids of UpdateAttribute
        updateattribute_id, updateattribute_version=get_processor_id(pg_id,"UpdateAttribute")
        print(updateattribute_id,updateattribute_version)

        #to get process ids of fetchs3Object
        FetchAzureDataLakeStorage_id, FetchAzureDataLakeStorage_version=get_processor_id(pg_id,"FetchAzureDataLakeStorage")
        print(FetchAzureDataLakeStorage_id,FetchAzureDataLakeStorage_version)

        set_azure_processor_props(ListAzureDataLakeStorage_id,"ListAzureDataLakeStorage",azure_controller_service_id,ListAzureDataLakeStorage_version)
        set_azure_processor_props(FetchAzureDataLakeStorage_id,"FetchAzureDataLakeStorage",azure_controller_service_id,FetchAzureDataLakeStorage_version)

        set_variables_publisher("sds-qasprint-datalake-ttv","sds-autoparser/rdm")

        delete_connection("FetchAzureDataLakeStorage",pg_id)
        x=-280
        y=432
        splitjson_id=add_splitjson(x,y)

        create_connection(FetchAzureDataLakeStorage_id,pg_id,"PROCESSOR",splitjson_id,pg_id,"PROCESSOR","success")
        create_connection(splitjson_id,pg_id,"PROCESSOR",updateattribute_id,pg_id,"PROCESSOR","split")

        #to get process ids of SplitJson
        splitjson_id, splitjson_version=get_processor_id(pg_id,"SplitJson")
        print(splitjson_id,splitjson_version)

        pg_status("RUNNING")
        time.sleep(10)
        pg_status("STOPPED")
        time.sleep(10)
        clearstate(ListAzureDataLakeStorage_id)

else:
    # to get the id of S3
    S3_id,S3_ver=get_pg_id(batch_id,'S3')
    print("S3 ID:", S3_id)

    # to get the id of Singleline
    singleline_id,singleline_ver=get_pg_id(S3_id,'singleline')
    print("Singleline ID:", singleline_id)

    # to get the id of Multiline
    multiline_id,multiline_ver=get_pg_id(S3_id,'multiline')
    print("Multiline ID:", multiline_id)

    funnel_id=get_funnel_id(S3_id,'s3-out')
    print("Funnel ID:", funnel_id)

    x=1796.7874635688343
    y=1622.7139261534476
    #creating a copy of singleline processor group
    pg_id=copy_pg(S3_id,singleline_id,"Defender",x,y)
    multi_pg_id=copy_pg(S3_id,multiline_id,"KnowledgeBase",x,y)

    #output port id of defender
    new_source1_out_id=get_output_port_id(pg_id,'new_source1-out')
    print("New source1 out ID:", new_source1_out_id)

    #to create connection between defender Processor_group and Funnel
    response_connection_funnel_pg=create_connection(new_source1_out_id,pg_id,"OUTPUT_PORT",funnel_id,S3_id,"FUNNEL")
    print(response_connection_funnel_pg)

    #output port id of knowledgebase
    new_source1_out_id=get_output_port_id(multi_pg_id,'new_source1-out')
    print("New source1 out ID of knowledgebase:", new_source1_out_id)

    #Connection between funnel and knowledgebase
    response_connection_funnel_pg=create_connection(new_source1_out_id,multi_pg_id,"OUTPUT_PORT",funnel_id,S3_id,"FUNNEL")
    print(response_connection_funnel_pg)

    aws_controller_service_id=get_creds_controller_service_id(pg_id)
    print(aws_controller_service_id)
    enable_pg_contoller_serverices("ENABLED")

    response_set_variables_pg=set_variables_pg(source_name,srdm_table_name,vendor,bucket_name,container,input_directory,source_path)
    print(response_set_variables_pg)

    #to get process ids of lists3
    lists3_id, lists3_version=get_processor_id(pg_id,"ListS3")
    print(lists3_id,lists3_version)

    #to get process ids of UpdateAttribute
    updateattribute_id, updateattribute_version=get_processor_id(pg_id,"UpdateAttribute")
    print(updateattribute_id,updateattribute_version)

    #to get process ids of fetchs3Object
    fetchs3_id, fetchs3_version=get_processor_id(pg_id,"FetchS3Object")
    print(fetchs3_id,fetchs3_version)

    #Setting the aws controller services id
    set_aws_processor_props(lists3_id,"ListS3",aws_controller_service_id,lists3_version)
    set_aws_processor_props(fetchs3_id,"FetchS3Object",aws_controller_service_id,fetchs3_version)

    #Setting the variables of publisher
    set_variables_publisher("pe-sds3-qasprint-datalake","sds-autoparser/rdm")

    #deleting the connection to incorporate splitJson
    delete_connection("FetchS3Object",pg_id)

    #adding processor splitJson
    x=408
    y=360
    splitjson_id=add_splitjson(x,y)

    #creating connection with SplitJson and other processors
    create_connection(fetchs3_id,pg_id,"PROCESSOR",splitjson_id,pg_id,"PROCESSOR","success")
    create_connection(splitjson_id,pg_id,"PROCESSOR",updateattribute_id,pg_id,"PROCESSOR","split")

    #to get process ids of SplitJson
    splitjson_id, splitjson_version=get_processor_id(pg_id,"SplitJson")
    print(splitjson_id,splitjson_version)

    publisher_s3_id,publisher_s3_ver=get_pg_id(publisher_id,'s3')
    PutS3Object_id, PutS3Object_version=get_processor_id(publisher_s3_id,"PutS3Object")
    
    #to set the region in publisher processor group
    set_publisher_processor_props(PutS3Object_id,"PutS3Object","us-east-2",PutS3Object_version)


    #For Knowledgebase data source

    #Removing the exisiting connection for Knowledgebase ds
    delete_connection("FetchS3Object",multi_pg_id)
    delete_connection("ReplaceText",multi_pg_id)
    delete_connection("UpdateAttribute",multi_pg_id)

    #fetching id's of the processors and the output port
    FetchS3Object_id,FetchS3Object_ver=get_processor_id(multi_pg_id,'FetchS3Object')
    UpdateAttribute_id, UpdateAttribute_version=get_processor_id(multi_pg_id,"UpdateAttribute")
    ReplaceText_id, ReplaceText_version=get_processor_id(multi_pg_id,"ReplaceText")

    new_source1_out_id=get_output_port_id(multi_pg_id,'new_source1-out')

    #reconnecting the processors after deleting the exisitng connection
    create_connection(FetchS3Object_id,multi_pg_id,"PROCESSOR",ReplaceText_id,multi_pg_id,"PROCESSOR","success")
    create_connection(UpdateAttribute_id,multi_pg_id,"PROCESSOR",new_source1_out_id,multi_pg_id,"OUTPUT_PORT","success")

    # adding a processor 'Replace Text'
    replace_text_id=add_replace_text_processor(multi_pg_id)
    replace_text_id, replace_text_version=get_processor_id(multi_pg_id,"ReplaceText_2")

    #reconnecting between the processors after a new 'Replace Text' processor is pulled
    create_connection(ReplaceText_id,multi_pg_id,"PROCESSOR",replace_text_id,multi_pg_id,"PROCESSOR","success")
    create_connection(replace_text_id,multi_pg_id,"PROCESSOR",UpdateAttribute_id,multi_pg_id,"PROCESSOR","success")

    #update the properties of the new 'Replace Text' processor
    update_replace_text_props(ReplaceText_id,ReplaceText_version)

    print("************Reached the end*********** ")  

    pg_status("RUNNING")
    time.sleep(10)
    pg_status("STOPPED")
    time.sleep(10)
    clearstate(lists3_id)      




# to check if file exists is RDM or not
# with open('azure_file_fetch.py', 'r') as file:
#     code = file.read()

# exec(code)
