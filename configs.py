def copy_pg(parent_pg_id,pg_id):
    payload={
    "disconnectedNodeAcknowledged": False,
    "snippet": {
        "parentGroupId": parent_pg_id,
        "processors": {},
        "funnels": {},
        "inputPorts": {},
        "outputPorts": {},
        "remoteProcessGroups": {},
        "processGroups": {
        pg_id: {
            "version": 0
        }
        },
        "connections": {},
        "labels": {}
    }
    }
    
    return payload

def paste_pg(snippet_id,x,y):
    
    payload={
        "snippetId": snippet_id,
        "originX": x,
        "originY": y,
        "disconnectedNodeAcknowledged": False
        }
    
    return payload

def rename_pg(name,pg_id,revision_version):
    payload={
        "revision": {
            "version": revision_version
        },
        "disconnectedNodeAcknowledged": False,
        "component": {
            "id": pg_id,
            "name": name,
            # "comments": "",
            # "parameterContext": {
            #   "id": null
            # },
            # "flowfileConcurrency": "UNBOUNDED",
            # "flowfileOutboundPolicy": "STREAM_WHEN_AVAILABLE",
            # "defaultFlowFileExpiration": "0 sec",
            # "defaultBackPressureObjectThreshold": "10000",
            # "defaultBackPressureDataSizeThreshold": "1 GB"
        }
        }
    return payload

def create_connection(source_id,source_gp_id,s_type,destination_id,dest_gp_id,d_type,relationship):
    payload = {
            "revision": {
                #"clientId": "6e7b7408-f4e3-172e-8316-1134b9cc08d4",
                "version": 0
            },
            "disconnectedNodeAcknowledged": False,
            "component": {
                "name": "",
                "source": {
                "id": source_id,
                "groupId": source_gp_id,
                "type": s_type
                },
                "destination": {
                "id": destination_id,
                "groupId": dest_gp_id,
                "type": d_type
                },
                "selectedRelationships": [
                relationship
                ],
                "flowFileExpiration": "0 sec",
                "backPressureDataSizeThreshold": "1 GB",
                "backPressureObjectThreshold": "10000",
                "bends": [],
                "prioritizers": [],
                "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
                "loadBalancePartitionAttribute": "",
                "loadBalanceCompression": "DO_NOT_COMPRESS"
            }
            }         
    return payload

def enable_pg_contoller_serverices(pg_id,state):
        payload= {
        "id": pg_id,
        "state": state,
        "disconnectedNodeAcknowledged": False
        }
        return payload
#####  made a change

def set_variables_pg(revision,pg_id,source_name,srdm_table_name,vendor,bucket_name,container,input_directory,source_path):
    payload = {
    "processGroupRevision": {
            "version": revision
        },
        "disconnectedNodeAcknowledged": False,
        "variableRegistry": {
            "processGroupId": pg_id,
            "variables": [
            {
                "variable": {
                "name": "bucket_name",
                "value": bucket_name
                }
            },
            {
                "variable": {
                "name": "container",
                "value": container
                }
            },
            {
                "variable": {
                "name": "input_directory",
                "value": input_directory
                }
            },
            {
                "variable": {
                "name": "source_name",
                "value": source_name
                }
            },
            {
                "variable": {
                "name": "source_path",
                "value": source_path
                }
            },
            {
                "variable": {
                "name": "srdm_table_name",
                "value": srdm_table_name
                }
            },
            {
                "variable": {
                "name": "vendor",
                "value": vendor
                }
            }
            ]
        }
        }
    return payload

def set_variables_publisher(publisher_ver,publisher_id,output_bucket,output_object_path):
    payload = {
        "processGroupRevision": {
            #"clientId": "41f33cc9-bba8-16ad-4d92-04252bbff833",
            "version": publisher_ver
        },
        "disconnectedNodeAcknowledged": False,
        "variableRegistry": {
            "processGroupId": publisher_id,
            "variables": [
            {
                "variable": {
                "name": "output_bucket",
                "value": output_bucket
                }
            },
            {
                "variable": {
                "name": "output_object_path",
                "value": output_object_path
                }
            }
            ]
        }
        }         
    return payload

def set_azure_controller_props(id,name,identity_client_id):
    payload={
        "component": {
            "id": id,
            "name": "ADLSCredentialsControllerService",
            "config": {
            "properties": {
                "storage-account-name": name,
                "storage-use-managed-identity": "true",
                "managed-identity-client-id": identity_client_id
            }
            },
            "state": "STOPPED"
        },
        "revision": {
            "version": 0
        },
        "disconnectedNodeAcknowledged": False
        }
    
    return payload

def set_azure_processor_props(id,name,service_id,version):
    payload={
        "component": {
            "id": id,
            "name": name,
            "config": {
            "properties": {
                "adls-credentials-service": service_id,
                # "Region": "us-east-2"
            }
            },
            "state": "STOPPED"
        },
        "revision": {
            "version": version
        },
        "disconnectedNodeAcknowledged": False
        }
    
    return payload

def set_aws_processor_props(id,name,service_id,version):
    payload={
        "component": {
            "id": id,
            "name": name,
            "config": {
            "properties": {
                "AWS Credentials Provider service": service_id,
                "Region": "us-east-2"
            }
            },
            "state": "STOPPED"
        },
        "revision": {
            "version": version
        },
        "disconnectedNodeAcknowledged": False
        }
    
    return payload

def set_publisher_processor_props(id,name,region,version):
    payload={
        "component": {
            "id": id,
            "name": name,
            "config": {
            "properties": {
                "Region": region
            }
            },
            "state": "STOPPED"
        },
        "revision": {
            #"clientId": "d9093835-b5d1-1297-5517-122cd75c9dc5",
            "version": version
        },
        "disconnectedNodeAcknowledged": False
        }
    
    return payload

def pg_pg(template_id,state):
    payload={
        "id": template_id,
        "state": state,
        "disconnectedNodeAcknowledged": False
        }
    return payload

def delete_connection(connection_vers):
    params={
         'version': connection_vers,
        'disconnectedNodeAcknowledged': 'False'
    }
    return params

def add_splitjson(x,y):
    payload = {
        "revision": {
            "version": 0
        },
        "disconnectedNodeAcknowledged": False,
        "component": {
            "type": "org.apache.nifi.processors.standard.SplitJson",
            "bundle": {
            "group": "org.apache.nifi",
            "artifact": "nifi-standard-nar",
            "version": "1.21.0"
            },
            "name": "SplitJson",
            "position": {
            "x": x,
            "y": y
            },
            "config": {
            "autoTerminatedRelationships": [
                    "failure",
                    "original"
                ],
            "properties": {
                "JsonPath Expression": "$.value",
                "Null Value Representation": "empty string"
            }
        }
        }   
    }      
    return payload

def add_replace_text_processor():
    payload = {
        "revision": {
            "version": 0
        },
        "disconnectedNodeAcknowledged": False,
        "component": {
            "type": "org.apache.nifi.processors.standard.ReplaceText",
            "bundle": {
            "group": "org.apache.nifi",
            "artifact": "nifi-standard-nar",
            "version": "1.21.0"
            },
            "name": "ReplaceText_2",
            "position": {
            "x": 1008,
            "y": 128
            },
            "config": {
            "properties": {
                "Replacement Strategy": "Regex Replace",
				"Replacement Value": "<ID>${'$1':trim()}</ID>",
                "Regular Expression": "<ID>(.*?)<\/ID>",
                "Character Set":"UTF-8",
                "Evaluation Mode":"Line-by-Line",
                "Line-by-Line Evaluation Mode":"All"
            },
            "autoTerminatedRelationships": [
                    "failure"
                ],
        }
        }   
    }      
    return payload

def update_replace_text_props(id,version):
    payload={
    "component": {
        "id": id,
        "name": "ReplaceText",
        "config": {
                "properties": {
                    "Regular Expression": "(?=^<VULN_LIST>)"
                }
            },
    },
    "revision": {
        "version": version
        }
    }
    return payload