import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))
from src.sparkcompare.utils.spark_compare import spark_compare
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


class compare_demo(spark_compare):
  _demo_schema = StructType([
    StructField("record_id", IntegerType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("amount", IntegerType(), nullable=False),
    StructField("description", StringType(), nullable=False),
  ])

  def __init__(self):
    super().__init__()
    schema_name = f"{self.getEnvName()}_gold.comparedemo"
    self.executionLogTable = f"{schema_name}.sparkcompare_execution_log"
    self.resultTable = f"{schema_name}.sparkcompare_test_results"

  def getSourceDataFrame(self) -> DataFrame:
    rows = [
      (record_id, f"category-{record_id % 5}", record_id * 10, f"record-{record_id}")
      for record_id in range(1, 101)
    ]
    return self._get_spark().createDataFrame(rows, schema=self._demo_schema)

  def getTargetDataFrame(self) -> DataFrame:
    rows = [
      (
        record_id,
        f"category-{record_id % 5}",
        record_id * 10 + (500 if record_id <= 10 else 0),
        f"record-{record_id}",
      )
      for record_id in range(1, 91)
    ]
    rows.extend(
      (record_id, f"category-{record_id % 5}", record_id * 10, f"record-{record_id}")
      for record_id in range(101, 111)
    )
    return self._get_spark().createDataFrame(rows, schema=self._demo_schema)
