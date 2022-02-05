package pl.codewerkz.sparky

import org.apache.spark.sql.{DataFrame, SaveMode}

class CDC extends SparkSessionWrapper {
  def run(): Unit = {
    import spark.implicits._

    val cdcDF = read("stg.people")
    // This is required in order to pull table data into spark so that read and write from and to the same table is possible.
    // Better approach would be writing to some staging table and then moving data to target table from which we read.
    cdcDF.cache().count()

    // insert
    val insertDataDF = List(
      (1, "Anczej", "some value"),
      (2, "Marian", "some value"),
      (3, "Gienowefa", "some value"),
      (4, "Zdzislawa", "some value"),
      (5, "Jebanczyk", "some value")
    ).toDF("customer_id", "name", "field")
    write(insertDataDF, "stg.people")

    // update
    val updateDataDF = List(
      (1, "Anczej", "some updated value"),
      (2, "Marian", "some updated value")
    ).toDF("customer_id", "name", "field")
    val updateOutputDF = cdcDF.as("old").join(updateDataDF.as("new"), $"old.customer_id" === $"new.customer_id", "anti").union(updateDataDF)
    write(updateOutputDF, "stg.people")

    // delete
    val deleteDataDF = List(
      (1, "Anczej", "some value"),
      (2, "Marian", "some value")
    ).toDF("customer_id", "name", "field")
    val deleteOutputDF = cdcDF.as("old").join(deleteDataDF.as("new"), $"old.customer_id" === $"new.customer_id", "anti")
    write(deleteOutputDF, "stg.people")
  }

  def read(table: String): DataFrame =
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost/cdc")
      .option("dbtable", table)
      .option("user", "postgres")
      .option("password", "bar")
      .load()

  def write(df: DataFrame, table: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    import spark.implicits._
    df.orderBy($"customer_id".asc)
      .write
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost/cdc")
      .option("dbtable", table)
      .option("user", "postgres")
      .option("password", "bar")
      .mode(saveMode)
      .save()
  }

}

object CDC {
  def main(args: Array[String]): Unit = {
    new CDC().run()
  }

}
