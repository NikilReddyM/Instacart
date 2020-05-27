import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types.StructType

trait Functions {
  def printSparkConfigToFile(spark:SparkSession,filePath:String):Unit={
    /*
    -input: takes sparksession and path to outputfile
    -output: nothing
    -function: prints spark config parameters to given file path
     */

    import spark.implicits._
    val confDF = spark.conf
      .getAll
      .toSeq
      .toDF("conf-parameter","conf-value")
      .coalesce(1)

    dataframeToCSV(confDF,filePath)

  }

  def csvToDataframe(spark:SparkSession,filePath:String):DataFrame={
    /*
    -input: takes sparksession and path to csv file that contain data
    -output: dataframe
    -function: prints spark config parameters to given file path
     */

    val df1 = spark.read
      .options(Map(
        "inferSchema"->"true",
        "header"->"true",
        "path"->s"$filePath"
      ))
      .csv()

    val df2 = df1.na.drop(minNonNulls = df1.columns.length)//drops rows if all cols are nulls
    df2
  }

  def csvToDataframe(spark:SparkSession,schema:StructType,filePath:String):DataFrame={
    /*
    -input: takes sparksession and path to csv file that contain data
    -output: dataframe
    -function: prints spark config parameters to given file path
     */

    val df1 = spark.read
        .schema(schema)
        .options(Map(
          //"mode"-> "DROPMALFORMED",
          // option "DROPMALFORMED" while loading the data which ignores corrupted records. but want to send bad records to another file
          //"mode"->"PERMISSIVE",
          //"badRecordsPath"-> "src/main/scala/instacart_analysis/badRecPath",//not working
          "header"->"true",
          "path"->s"$filePath"
        ))
        .csv()

    /*

    got problem: schema is not getting enforced for nullable
    solved-
    https://stackoverflow.com/questions/54433142/spark-scala-schema-not-enforced-on-load
    https://stackoverflow.com/questions/41705602/spark-dataframe-schema-nullable-fields
     */


    val df2 = df1.na.drop(minNonNulls = df1.columns.length)//drops rows if all cols are nulls
    df2
  }

  def dataframeToCSV(df:DataFrame,filePath:String):Unit={
    /*
        -input: takes dataframe and path where the dataframe should be stored as csv file
        -output: nothing
        -function: writes dataframe to given file path
         */
    df.write
      .format("csv")
      .options(Map(
        "header"->"true",
        "path"->s"$filePath"))
      .save()

  }



}
