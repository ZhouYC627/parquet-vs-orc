import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}



object ParquetDemo {
  def main(args: Array[String]): Unit = {
    println("HelloWorld!")

    val spark = SparkSession.builder();
    val conf = new SparkConf().setAppName("JSONDataSource")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
    sqlContext.setConf("spark.sql.orc.impl", "native")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    sqlContext.sql("use ssb_20")
    sqlContext.sql("show tables").show()

    val customer = hiveContext.table("customer")
    val dates = hiveContext.table("dates")
    val supplier = hiveContext.table("supplier")
    val part = hiveContext.table("part")
    val p_lineorder = hiveContext.table("p_lineorder")
    customer.write.mode("overwrite").parquet("customer.parquet")
    dates.write.mode("overwrite").parquet("dates.parquet")
    supplier.write.mode("overwrite").parquet("supplier.parquet")
    part.write.mode("overwrite").parquet("part.parquet")
    p_lineorder.write.mode("overwrite").parquet("p_lineorder.parquet")

    spark.read.parquet("part.parquet").createOrReplaceTempView("part_parquet")
    spark.read.parquet("customer.parquet").createOrReplaceTempView("customer_parquet")
    spark.read.parquet("supplier.parquet").createOrReplaceTempView("supplier_parquet")
    spark.read.parquet("dates.parquet").createOrReplaceTempView("dates_parquet")
    spark.read.parquet("p_lineorder.parquet").createOrReplaceTempView("p_lineorder_parquet")

    //query: spark.sql("...")
  }
}
