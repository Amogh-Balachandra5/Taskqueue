package Utils

import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkSession = SparkSession.builder()
    .appName("TaskQueue")
    .getOrCreate()
}