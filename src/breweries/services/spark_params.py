from typing import Dict

ADDITIONAL_PARAMETERS: Dict = {
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.executor.memory": "70g",
    "spark.driver.memory": "50g",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "16g",
    "spark.driver.extraJavaOptions": "-Xms20g",
    "spark.databricks.delta.schema.autoMerge.enabled": "true"
}
