#!/usr/bin/env python

from pathlib import Path
from random import randint
from http.server import BaseHTTPRequestHandler, HTTPServer
import socketserver,subprocess,os,requests,json,time,shutil
import cepCode, logging

class S():
    def execute_maven(self):
        print(os.getcwd())
        logging.info("Packaging program with maven")
        mypath = './prediction-job'
        os.chdir(mypath)
        p = subprocess.Popen(["mvn package"],shell=True, stdout = subprocess.PIPE)
        output, err = p.communicate()
        os.chdir("..")
        logging.info(output)
        return mypath


if __name__ == "__main__":

    print("Spark Jar generated")
    S().execute_maven()
    #jar_id = self.upload_jar(directory, flink_endpoint)
    #self.run_job(directory, spark_endpoint)
    #self.delete_jar(jar_id, flink_endpoint)
    #self._set_headers()

