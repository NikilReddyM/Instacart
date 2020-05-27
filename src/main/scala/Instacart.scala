import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Instacart extends Functions with Schemas with QueryFunctions
  {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("Instacart_Analysis")
        //.config("spark.master","local")
        .getOrCreate()

      val pathToDataDIR = args(0)
      val pathToDataOutputDIR = args(1)

      val configFile = "spark-configurations"
      val aisleFile = "aisles.csv"
      val departmentsFile = "departments.csv"
      val ordersFile = "orders.csv"
      val productsFile = "products.csv"

      val aislesDF = broadcast(csvToDataframe(spark, aisleSchema, pathToDataDIR+aisleFile))
      val departmentsDF = broadcast(csvToDataframe(spark,departmentsSchema,pathToDataDIR+departmentsFile))
      val ordersDF = csvToDataframe(spark,pathToDataDIR+ordersFile)
      val productsDF = csvToDataframe(spark,productsSchema,pathToDataDIR+productsFile)

      printSparkConfigToFile(spark,pathToDataOutputDIR+configFile)
//      dataframeToCSV(aislesDF,pathToDataOutputDIR+"aislesDF")
//      dataframeToCSV(departmentsDF,pathToDataOutputDIR+"departmentsDF")
//      dataframeToCSV(ordersDF,pathToDataOutputDIR+"ordersDF")
//      dataframeToCSV(productsDF,pathToDataOutputDIR+"productsDF")

      val q1DF = Q1(productsDF,"department_id")
      dataframeToCSV(q1DF,pathToDataOutputDIR+"out_Q1")

        q1DF.persist()

        val q2DF = Q2(q1DF,"products_count")
        dataframeToCSV(q2DF,pathToDataOutputDIR+"out_Q2")

        q1DF.unpersist()

        val q3DF = Q3(productsDF,"department_id","aisle_id","department_aisles")
        dataframeToCSV(q3DF,pathToDataOutputDIR+"out_Q3")

        q3DF.persist()

        val q4DF = Q4(q3DF,"department_aisles","aisle_department_count")
        dataframeToCSV(q4DF,pathToDataOutputDIR+"out_Q4")

        q3DF.unpersist()
        q4DF.persist()

        val q5DF = Q5(q4DF,"aisle_department_count")
        dataframeToCSV(q5DF,pathToDataOutputDIR+"out_Q5")

        q4DF.unpersist()

        val q7Count = Q7(ordersDF,"user_id")
        println(s"number of users is: $q7Count")

        val q8DF = Q8(ordersDF,"user_id","orders_count")
        dataframeToCSV(q8DF,pathToDataOutputDIR+"out_Q8")

        q8DF.persist()

        val q9DF = Q9(q8DF,"orders_count")
        dataframeToCSV(q9DF,pathToDataOutputDIR+"out_Q9")

        q8DF.unpersist()

        val q10DF = Q10(ordersDF)
        dataframeToCSV(q10DF,pathToDataOutputDIR+"out_Q10")

        val q11DF = Q11(ordersDF)
        dataframeToCSV(q11DF,pathToDataOutputDIR+"out_Q11")

        val q12DF = Q12(ordersDF)//still incomplete
        dataframeToCSV(q12DF,pathToDataOutputDIR+"out_Q12")

        val q13DF = Q13(ordersDF)//still incomplete
        dataframeToCSV(q13DF,pathToDataOutputDIR+"out_Q13")
    }
  }


