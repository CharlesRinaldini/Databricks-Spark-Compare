#import for using dbutils in python
# from databricks.sdk.runtime import *
from databricks.sdk import WorkspaceClient
from datetime import datetime
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

useDatabricks = os.getenv('ENV') == "dv" 

if useDatabricks:
    from databricks.connect import DatabricksSession as SparkSession
    os.environ["USER"] = "cibuild"
else:
    from pyspark.sql import SparkSession

from pathlib import Path
#imports for sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
#datetime functions for folder and file naming 
from datetime import datetime
#import for creating output of notebook
import json
#import for interacting with Delta Lake
import delta
#import for creating unique temp table name
import uuid
#import for http requests
import requests
import base64
#for parallel running of notebooks
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count

def printWithTime(str):
  # Print a string with the current timestamp
  now = datetime.now()
  modifiedDatetime = now.strftime("%Y-%m-%d %H:%M:%S")
  printWithTime(f"{modifiedDatetime}: {str}")

class compare_helper:
  def __init__(self):
    #set variables use later
    osENV = os.getenv('ENV')
    if osENV is None:
       osENV = "dv"

    self.setSpark()

    try:
      path = Path(__file__)
      configPath = path.parent.parent.parent.parent.absolute()
      with open(os.path.join(configPath, "config", f"{osENV}-config.json")) as f: 
        envConfig = (json.load(f))["envConfig"]
        
      scopeName = envConfig["scopeName"]
      # Set paths
      envName = envConfig["envName"]

      dbrURL = envConfig["databricksURL"]
      dbrTokenSecret = envConfig["databricksTokenSecret"]

      now = datetime.now()
      self.databricksURL = dbrURL
      self.databricksTokenSecret = dbrTokenSecret
      self.envName = envName

    except BaseException as e:
      if "Attribute `sparkContext` is not supported" in str(e):
        raise Exception("SparkContext is not available. Please run this notebook on a single user UC enabled cluster.")
      else: 
        raise Exception(str(e))
      
  def setSpark(self) -> SparkSession:
    self.spark = SparkSession.builder.getOrCreate()
    return self.spark
  
  def getSpark(self):
    return self.spark  
  
  def getDbutils(self):
    return self.workspaceClient.dbutils
  
  def getEnvName(self):
    # Get the environment's name
    return self.envName
