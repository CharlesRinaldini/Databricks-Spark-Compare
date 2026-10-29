import sys
import os
from pathlib import Path
filePath = Path(__file__)
utilsPath = filePath.parent.parent.absolute()
sys.path.append(os.path.abspath(utilsPath))
from utils.compare_helper import *
 
class sqldemo(compare_helper):
  def __init__(self):
    super().__init__()
    spark = self.setSpark()    

  def getDataFrame(self):
    envName = self.getEnvName()
    spark = self.getSpark()

    df = spark.sql(f"SELECT * FROM {envName}_gold.sqldemo.demo_table")
    return df