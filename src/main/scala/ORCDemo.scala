import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object ORCDemo {
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
    customer.write.mode("overwrite").format("orc").save("customer.orc")
    dates.write.mode("overwrite").format("orc").save("dates.orc")
    supplier.write.mode("overwrite").format("orc").save("supplier.orc")
    part.write.mode("overwrite").format("orc").save("part.orc")
    p_lineorder.write.mode("overwrite").format("orc").save("p_lineorder.orc")

    spark.read.orc("part.orc").createOrReplaceTempView("part_orc")
    spark.read.orc("customer.orc").createOrReplaceTempView("customer_orc")
    spark.read.orc("supplier.orc").createOrReplaceTempView("supplier_orc")
    spark.read.orc("dates.orc").createOrReplaceTempView("dates_orc")
    spark.read.orc("p_lineorder.orc").createOrReplaceTempView("p_lineorder_orc")

    //query: spark.sql("...")
  }
}
