#!/usr/bin/env python


import socketserver,subprocess,os,requests,json,time,shutil
import logging

class S():
    def execute_maven(self):
        print(os.getcwd())
        logging.info("Packaging program with maven")
        mypath = './prediction-job'
        os.chdir(mypath)
        p = subprocess.Popen(["mvn package"],shell=True, stdout = subprocess.PIPE)
        output, err = p.communicate()
        os.chdir("..")
        print(output)
        logging.info(output)
        print(mypath)
        return mypath


if __name__ == "__main__":

    print("Spark Jar generated")
    S().execute_maven()


