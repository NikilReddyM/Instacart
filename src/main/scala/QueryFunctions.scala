import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


trait QueryFunctions {

  def Q1(dfOriginal:DataFrame, columnName:String):DataFrame={
    val df1 = dfOriginal
      .coalesce(1)
      .groupBy(columnName)
      .count()

    val df2: DataFrame = df1.withColumnRenamed("count","products_count")

    df2
  }

  def Q2(dfOriginal:DataFrame,columnName:String):DataFrame={
    val df1 = dfOriginal
      .coalesce(1)
      .sort(columnName)
    df1
  }

  def Q3(dfOriginal:DataFrame,col1:String,col2:String,col3:String):DataFrame={
    val df1 = dfOriginal
      .groupBy(col1)
      .agg(concat_ws("\t",collect_set(col2)).as(col3))

    val df2 = df1.coalesce(1)

    df2
  }

  def Q4(dfOriginal:DataFrame,col1:String,col2:String):DataFrame={
    val df1 = dfOriginal
        .withColumn(col2,size(split(col(col1),"\t")))
        .drop(col1)
        .coalesce(1)
        .toDF()

    df1
  }

  def Q5(dfOriginal:DataFrame,col1:String):DataFrame={
    val df1 = dfOriginal
      .coalesce(1)
      .sort(desc(col1))

    df1
  }

  def Q7(dfOriginal:DataFrame,col1:String)={
    val df1 = dfOriginal.select(col1)
      .dropDuplicates()

    val count = df1.count()
    count
  }

  def Q8(dfOriginal:DataFrame,col1:String,col2:String):DataFrame={
    val df1 = dfOriginal.select("user_id")
          .groupBy(col1)
          .agg(count(col1).as(col2))
          .coalesce(1)
        .toDF()

    df1
  }

  def Q9(dfOriginal:DataFrame,col1:String):DataFrame={
    val df1 = dfOriginal.sort(desc(col1))

    val df2 = df1.toDF()

    df2
  }

  def Q10(dfOriginal:DataFrame):DataFrame={
    val df1 = dfOriginal.select("order_dow")
          .groupBy("order_dow")
          .agg(count("order_dow").as("day_orders_count"))
            .coalesce(1)
    val df2 = df1.sort(desc("day_orders_count")).toDF()

    df2
  }

  def Q11(dfOriginal:DataFrame):DataFrame={
    val df1 = dfOriginal.select("order_hour_of_day")
          .groupBy("order_hour_of_day")
          .agg(count("order_hour_of_day").as("orders_count"))
          .coalesce(1)

    val df2 = df1.sort(asc("order_hour_of_day")).toDF()

    df2
  }

  def Q12(dfOriginal:DataFrame):DataFrame={

    val df1 = dfOriginal.select("order_dow","order_hour_of_day")
          .groupBy("order_dow","order_hour_of_day")
          .agg(count("order_dow").as("day_hour_count"))
          .orderBy("order_dow","order_hour_of_day")

    val window_dow = Window.partitionBy("order_dow")


    val df2 = df1.coalesce(1)
      .withColumn("min_week_count",min("day_hour_count") over window_dow)
            .withColumn("max_week_count",max("day_hour_count") over window_dow)
            .filter(
              (col("day_hour_count")===col("min_week_count"))
                or
                (col("day_hour_count")===col("max_week_count"))
            )
        .toDF()

    df2
  }


  def Q13(dfOriginal:DataFrame):DataFrame={
    val df1 = dfOriginal.select("order_dow","order_hour_of_day")
          .groupBy("order_dow","order_hour_of_day")
          .agg(count("order_dow").as("orders_in_hour"))
          .coalesce(1)

    val window_dow = Window.partitionBy("order_dow").orderBy(desc("orders_in_hour"))

    val df2 = df1.withColumn("dow_orders_rank",rank() over window_dow)

    val df3 = df2.filter(col("dow_orders_rank")<=5).toDF()

    df3
  }



}
/*
    Q1: product count by each department
    Q2: product count in each department with sorted order of department by product count
    Q3: aisles used for each department
    Q4: does any aisle have products related to more than 1 department?
    Q5: which department covers more number of aisles
    Q6: is there any relation between number of aisles for department and number of products in department.
    Q7: how many users?
    Q8: orders purchased  per each user
    Q9: sorted list of users by orders count
    Q10: which day of the week has more customers--sorted
    Q11: customers in each hour
    Q12: in each day of the week, what is the min and max orders  by hour
    Q13: in each day of the week, what are the hours with top 5 order count
    Q14: in each day of the week, what are the hours with least 5 order count
 */
