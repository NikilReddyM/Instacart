import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

trait Schemas {
  val aisleSchema = StructType(Seq(
    StructField("aisle_id",IntegerType,nullable = false),
    StructField("aisle",StringType,nullable = false)
  ))

  val departmentsSchema = StructType(Seq(
    StructField("department_id",IntegerType,nullable = false),
    StructField("department",StringType,nullable = false)
  ))

  val ordersSchema=StructType(Seq(
    StructField("order_id",IntegerType,nullable = true),
    StructField("user_id",IntegerType,nullable = true),
    StructField("eval_set",StringType,nullable = true),
    StructField("order_number",IntegerType,nullable = true),
    StructField("order_dow",IntegerType,nullable = true),
    StructField("order_hour_of_day",IntegerType,nullable = true),
    StructField("days_since_prior_order",IntegerType,nullable = true)
  ))

  val productsSchema = StructType(Seq(
    StructField("product_id",IntegerType,nullable = true),
    StructField("product_name",StringType,nullable = true),
    StructField("aisle_id",IntegerType,nullable = true),
    StructField("department_id",IntegerType,nullable = true)
  ))


}
