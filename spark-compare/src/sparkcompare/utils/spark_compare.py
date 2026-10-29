from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent / "src"))
import json
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence
from gresearch.spark.diff import DiffOptions
from pyspark.sql import functions as fn
from pyspark.sql.functions import col, lit, struct, to_json, trim, when

if TYPE_CHECKING:
  from pyspark.sql import DataFrame


def printWithTime(message: str) -> None:
  now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  print(f"{now}: {message}")


def _json_compatible(value: Any) -> Any:
  if hasattr(value, "asDict"):
    value = value.asDict(recursive=True)
  if isinstance(value, dict):
    if "jsonBody" in value and len(value) == 1 and isinstance(value["jsonBody"], str):
      try:
        return json.loads(value["jsonBody"])
      except json.JSONDecodeError:
        return value
    return {str(key): _json_compatible(item) for key, item in value.items()}
  if isinstance(value, (list, tuple)):
    return [_json_compatible(item) for item in value]
  return value


@dataclass
class DataFrameComparisonResult:
  """Serializable outcome and sample differences from a DataFrame comparison."""

  comparison_id: str
  test_case_name: str
  test_case_id: str | None
  parent_log_id: str | None
  status: str
  source_count: int
  target_count: int
  difference_count: int | None
  schema_matches: bool
  schema_differences: dict[str, Any]
  key_columns: list[str]
  samples: list[dict[str, Any]]
  started_at: str
  completed_at: str

  def to_dict(self) -> dict[str, Any]:
    return asdict(self)

  def to_json(self) -> str:
    return json.dumps(self.to_dict(), default=str, sort_keys=True)


class spark_compare:
  """Compare Spark DataFrames and append execution details to Databricks tables.

  Comparisons use multiset semantics, so duplicate rows are counted. The source
  and target must have the same columns and compatible data types; column order
  may differ. ``key_columns`` are retained as comparison metadata and included
  in the difference samples, while row equality remains exact.

  The table names can be unqualified (current catalog/schema) or fully
  qualified. Databricks creates the managed tables on the first append.
  """

  def __init__(
    self,
    spark: Any | None = None,
    execution_log_table: str = "sparkcompare_execution_log",
    result_table: str = "sparkcompare_test_results",
  ):
    self.spark = spark
    self.executionLogTable = self._validate_table_name(execution_log_table)
    self.resultTable = self._validate_table_name(result_table)
    self.last_result: DataFrameComparisonResult | None = None
    self.diffDataFrame: DataFrame | None = None

    os_env = os.getenv("ENV") or "dv"
    try:
      config_path = Path(__file__).parent.parent.parent.parent / "config" / f"{os_env}-config.json"
      with config_path.open(encoding="utf-8") as config_file:
        env_config = json.load(config_file)["envConfig"]

      self.envName = env_config["envName"]
      self.databricksURL = env_config["databricksURL"]
      self.databricksToken = env_config["databricksToken"]
      self.databricksHTTPPath = env_config["databricksHTTPPath"]
      self.databricksHost = env_config["databricksHost"]
    except Exception as error:
      if "Attribute `sparkContext` is not supported" in str(error):
        raise RuntimeError(
          "SparkContext is not available. Please run this notebook on a "
          "single user UC enabled cluster."
        ) from error
      raise RuntimeError(f"Unable to load Databricks configuration: {error}") from error

  @staticmethod
  def _validate_table_name(table_name: str) -> str:
    if not isinstance(table_name, str) or not table_name.strip():
      raise ValueError("Databricks table names must be non-empty strings.")
    return table_name.strip()

  @staticmethod
  def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()

  @staticmethod
  def _as_identifier(value: Any) -> str | None:
    return None if value is None else str(value)

  def _get_spark(self) -> Any:
    if self.spark is None:
      from pyspark.sql import SparkSession

      self.spark = SparkSession.builder.getOrCreate()
    return self.spark

  def setDiffDataFrame(self, diff_df: DataFrame | None) -> None:
    self.diffDataFrame = diff_df

  def getDiffDataFrame(self) -> DataFrame | None:
    return self.diffDataFrame

  def _append_table_record(self, table_name: str, record: dict[str, Any]) -> None:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    schema = StructType([
      StructField("event_id", StringType(), False),
      StructField("comparison_id", StringType(), False),
      StructField("test_case_id", StringType(), True),
      StructField("test_case_name", StringType(), True),
      StructField("parent_log_id", StringType(), True),
      StructField("event_type", StringType(), False),
      StructField("status", StringType(), True),
      StructField("error_code", StringType(), True),
      StructField("error_description", StringType(), True),
      StructField("notes", StringType(), True),
      StructField("workload_url", StringType(), True),
      StructField("source_record_count", LongType(), True),
      StructField("target_record_count", LongType(), True),
      StructField("error_count", LongType(), True),
      StructField("results_json", StringType(), True),
      StructField("created_at", StringType(), False),
    ])
    row = {
      "event_id": str(uuid.uuid4()),
      "comparison_id": "",
      "test_case_id": None,
      "test_case_name": None,
      "parent_log_id": None,
      "event_type": "",
      "status": None,
      "error_code": None,
      "error_description": None,
      "notes": None,
      "workload_url": None,
      "source_record_count": None,
      "target_record_count": None,
      "error_count": None,
      "results_json": None,
      "created_at": self._timestamp(),
    }
    row.update(record)
    self._get_spark().createDataFrame([row], schema=schema).write.format("delta").mode("append").saveAsTable(table_name)

  def compare_dataframes(
    self,
    source_df: DataFrame,
    target_df: DataFrame,
    test_case_name: str,
    test_case_id: Any = None,
    parent_log_id: Any = None,
    workload_url: str | None = None,
    join_columns: str | Sequence[str] | None = None,
    error_sampling_size: int = 100,
    execution_log_id: Any = None,
  ) -> DataFrameComparisonResult:
    """Compare DataFrames using gresearch's keyed diff and persist the results."""

    if error_sampling_size < 0:
      raise ValueError("error_sampling_size cannot be negative.")
    if not test_case_name:
      raise ValueError("test_case_name must be provided.")

    if join_columns is None:
      keys: list[str] = []
    elif isinstance(join_columns, str):
      keys = [column.strip() for column in join_columns.replace(" ", "").split(",") if column.strip()]
    else:
      keys = [str(column).strip() for column in join_columns if str(column).strip()]
    keys = list(dict.fromkeys(keys))
    if not keys:
      raise ValueError("At least one join column must be provided.")

    self.spark = source_df.sparkSession
    comparison_id = self._as_identifier(execution_log_id) or self.insertExecutionLog(
      test_case_id,
      parent_log_id,
      f"{test_case_name} start",
      workload_url or "",
    )
    started_at = self._timestamp()
    source_compare_df = source_df.select([
      trim(col(column).cast("string")).alias(column) for column in source_df.columns
    ]).cache()
    target_compare_df = target_df.select([
      trim(col(column).cast("string")).alias(column) for column in target_df.columns
    ]).cache()
    changes_diff_df = None
    temp_view_name = None

    try:
      printWithTime("Counting source rows")
      source_count = int(source_compare_df.count())
      printWithTime("Counting target rows")
      target_count = int(target_compare_df.count())

      options = DiffOptions().with_change_column("changes")
      diff_df = source_compare_df.diff_with_options(
        target_compare_df, options, *keys
      ).orderBy(*keys)
      self.setDiffDataFrame(diff_df)
      changes_diff_df = diff_df.filter("diff = 'C'")
      missing_diff_df = diff_df.filter("diff in ('D', 'I')")
      missing_count = int(missing_diff_df.count())
      missing_samples_df = missing_diff_df.select(
        *keys,
        when(missing_diff_df["diff"] == "D", "Missing in Target")
        .otherwise("Missing in Source")
        .alias("diffFieldName"),
        lit(None).alias("source"),
        lit(None).alias("target"),
      ).limit(100)

      printWithTime("Comparing sets")
      changes_diff_df = changes_diff_df.cache()
      changed_count = int(changes_diff_df.count())
      value_columns = changes_diff_df.drop(*keys).drop("diff", "changes").columns

      if value_columns:
        unpivot_df = changes_diff_df.unpivot(
          keys, value_columns, "fieldName", "value"
        )
        view_name = f"upDF_{uuid.uuid4().hex}"
        unpivot_df.createOrReplaceTempView(view_name)
        temp_view_name = view_name
        unpivot_df2 = self.spark.sql(f"""
          select *
          , case when fieldName like 'left_%' then 'source' else 'target' end as sourceName
          , replace(replace(fieldName, 'left_', ''), 'right_', '') as diffFieldName
          from `{temp_view_name}`
        """).drop("fieldName")
        group_keys = [*keys, "diffFieldName"]
        pivoted_diff_df = (
          unpivot_df2.groupBy(*group_keys)
          .pivot("sourceName", ["source", "target"])
          .agg(fn.first("value"))
        )
        #limit to 100 rows for sampling, but only if there are differences
        filtered_pivoted_diff_df = pivoted_diff_df.filter(
          "trim(coalesce(source, '')) != trim(coalesce(target, ''))"
        ).limit(100)
      else:
        filtered_pivoted_diff_df = (
          changes_diff_df.select(*keys)
          .limit(0)
          .withColumn("diffFieldName", lit(None).cast("string"))
          .withColumn("source", lit(None).cast("string"))
          .withColumn("target", lit(None).cast("string"))
        )

      combined_diff_dfs = filtered_pivoted_diff_df.unionByName(missing_samples_df)
      encoded_json_df = combined_diff_dfs.withColumn(
        "jsonBody", to_json(struct(*[col(column) for column in combined_diff_dfs.columns]))
      )
      error_count = changed_count + missing_count

      printWithTime("Writing to log")
      if error_count == 0:
        notes = f"{test_case_name} ran finished, Result: {test_case_name} Passed!"
        self.updateExecutionLog(comparison_id, 1, "", "", notes)
        encoded_json_list: list[Any] = []
      else:
        notes = (
          f"{test_case_name} ran finished, Result: {test_case_name} failed, "
          "for details check the Databricks results table."
        )
        self.updateExecutionLog(
          comparison_id, 0, "1001", "Query result not matching", notes
        )
        if error_sampling_size > 0:
          encoded_json_list = encoded_json_df.select("jsonBody").head(error_sampling_size)
        else:
          encoded_json_list = encoded_json_df.select("jsonBody").collect()

      completed_at = self._timestamp()
      samples = [_json_compatible(row) for row in encoded_json_list]
      result = DataFrameComparisonResult(
        comparison_id=comparison_id,
        test_case_name=test_case_name,
        test_case_id=self._as_identifier(test_case_id),
        parent_log_id=self._as_identifier(parent_log_id),
        status="PASSED" if error_count == 0 else "FAILED",
        source_count=source_count,
        target_count=target_count,
        difference_count=error_count,
        schema_matches=True,
        schema_differences={},
        key_columns=keys,
        samples=samples,
        started_at=started_at,
        completed_at=completed_at,
      )
      self.last_result = result
      self.saveTestResult(
        comparison_id,
        encoded_json_list,
        source_count,
        target_count,
        error_count,
        test_case_name=test_case_name,
        test_case_id=test_case_id,
        parent_log_id=parent_log_id,
        status=result.status,
      )
      return result
    finally:
      if temp_view_name is not None:
        self.spark.catalog.dropTempView(temp_view_name)

  def insertExecutionLog(self, i_testCaseId: Any, i_parentId: Any, i_notes: str, i_workLoadURL: str) -> str:
    comparison_id = str(uuid.uuid4())
    self._append_table_record(self.executionLogTable, {
      "comparison_id": comparison_id,
      "test_case_id": self._as_identifier(i_testCaseId),
      "parent_log_id": self._as_identifier(i_parentId),
      "event_type": "EXECUTION_STARTED",
      "status": "RUNNING",
      "notes": i_notes,
      "workload_url": i_workLoadURL,
    })
    return comparison_id

  def updateExecutionLog(
    self,
    i_testExecutionLogId: Any,
    i_passFailFlag: int,
    i_errorCode: str,
    i_errorDesc: str,
    i_notes: str,
  ) -> None:
    self._append_table_record(self.executionLogTable, {
      "comparison_id": str(i_testExecutionLogId),
      "event_type": "EXECUTION_UPDATED",
      "status": "PASSED" if i_passFailFlag else "FAILED",
      "error_code": i_errorCode,
      "error_description": i_errorDesc,
      "notes": i_notes,
    })

  def saveTestResult(
    self,
    i_testExecutionLogId: Any,
    i_jsonResults: Any,
    i_SourceRecordCount: int,
    i_TargetRecordCount: int,
    i_ErrorCount: int,
    *,
    test_case_name: str | None = None,
    test_case_id: Any = None,
    parent_log_id: Any = None,
    status: str | None = None,
  ) -> None:
    if isinstance(i_jsonResults, str):
      try:
        json_results = json.loads(i_jsonResults) if i_jsonResults.strip() else []
      except json.JSONDecodeError:
        json_results = i_jsonResults
    else:
      json_results = i_jsonResults
    results_json = json.dumps(_json_compatible(json_results), default=str, sort_keys=True)

    self._append_table_record(self.resultTable, {
      "comparison_id": str(i_testExecutionLogId),
      "test_case_id": self._as_identifier(test_case_id),
      "test_case_name": test_case_name,
      "parent_log_id": self._as_identifier(parent_log_id),
      "event_type": "TEST_RESULT",
      "status": status,
      "source_record_count": int(i_SourceRecordCount),
      "target_record_count": int(i_TargetRecordCount),
      "error_count": int(i_ErrorCount),
      "results_json": results_json,
    })

  def getLastResultJson(self) -> str | None:
    return None if self.last_result is None else self.last_result.to_json()

  def getEnvName(self) -> str:
    return self.envName

  def getDatabricksHTTPPath(self) -> str:
    return self.databricksHTTPPath

  def getDatabricksHost(self) -> str:
    return self.databricksHost

  def getDatabricksURL(self) -> str:
    return self.databricksURL

  def getDatabricksToken(self) -> str:
    return self.databricksToken
