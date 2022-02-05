package pl.codewerkz.sparky

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("sparky").getOrCreate()
  }

}
