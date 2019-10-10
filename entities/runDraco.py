#!/usr/bin/env python
# coding: utf-8

# In[41]:
import os, requests,json, time
import logging

class RunDraco():

    def get_template_info(self, draco_endpoint):
        print("Get Templates")
        DRACO_ENDPOINT = "http://" + draco_endpoint + "/nifi-api/flow/templates"
        r = requests.get(url = DRACO_ENDPOINT) 
        response = json.loads(r.text)
        logging.info("Response:%s "%response)
        group_id=""
        template_id=""
        for templates in response["templates"]:
             if templates["template"]["name"]=="ORION-TO-MONGO":
                group_id=templates["template"]["groupId"]
                template_id=templates["template"]["id"]
        print(group_id)   
        print(template_id)  
        r = requests.get(url = DRACO_ENDPOINT) 
        return [group_id,template_id]
    
    def put_template(self, draco_endpoint,group_id,template_id):
        print("Template Init")
        DRACO_ENDPOINT = "http://" + draco_endpoint + "/nifi-api/process-groups/"+group_id+"/template-instance"
        headers = {'content-type': 'application/json'}
        data = {"originX": 5.0,
                "originY": 0.0,
                "templateId":template_id
               }
        r = requests.post(url = DRACO_ENDPOINT, json=data ,headers=headers) 
        response = json.loads(r.text)
        print("Response:%s "%response)
    
    def get_processors_id(self, draco_endpoint,group_id):
        print("Get Processors id")
        DRACO_ENDPOINT = "http://" + draco_endpoint + "/nifi-api/process-groups/"+group_id+"/processors"
        r = requests.get(url = DRACO_ENDPOINT) 
        processors_id=[]
        response = json.loads(r.text)
        for processor in response["processors"]:
            processors_id.append(processor["component"]["id"])
        return processors_id

    def run_processors(self,draco_endpoint,list_processors):
        print("Run Processors")
        headers = {'content-type': 'application/json'}
        for processor in list_processors:
            DRACO_ENDPOINT = "http://" + draco_endpoint + "/nifi-api/processors/"+processor+"/run-status"
            
            data = {
                "revision": {
                    "clientId": "8bb725ef-0158-1000-478b-da5903184809",
                        "version": 0
                },
                "state": "RUNNING"
            }
            r = requests.put(url = DRACO_ENDPOINT, json=data ,headers=headers) 
            response = json.loads(r.text)
            print("Response:%s "%response)

if __name__ == "__main__":
    
    from sys import argv
    draco_endpoint = os.environ['DRACO_ENDPOINT']
    if draco_endpoint=="":
        draco_endpoint = "draco:9090"
    init=RunDraco()
    time.sleep(40)
    template_info=init.get_template_info(draco_endpoint)
    group_id=template_info[0]
    init.put_template(draco_endpoint,group_id,template_info[1])
    processors_id=init.get_processors_id(draco_endpoint,group_id)
    init.run_processors(draco_endpoint,processors_id ) 
     
    
    


# In[ ]:




