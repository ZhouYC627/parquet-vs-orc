import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object ORCDemo {
  def main(args: Array[String]): Unit = {
    println("HelloWorld!")

    val spark = SparkSession.builder();
    val conf = new SparkConf().setAppName("JSONDataSource")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    sqlContext.sql("show tables").show()
    sqlContext.sql("show tables").show()

    val customer = hiveContext.table("customer")
    customer.write.orc("customer.orc")
    val dates = hiveContext.table("dates")
    dates.write.orc("dates.orc")
    val supplier = hiveContext.table("supplier")
    supplier.write.orc("supplier.orc")
    val part = hiveContext.table("part")
    part.write.orc("part.orc")
    val p_lineorder = hiveContext.table("p_lineorder")
    p_lineorder.write.orc("p_lineorder.orc")

    spark.read.orc("part.orc").createOrReplaceTempView("part_orc")
    spark.read.orc("customer.orc").createOrReplaceTempView("customer_orc")
    spark.read.orc("supplier.orc").createOrReplaceTempView("supplier_orc")
    spark.read.orc("dates.orc").createOrReplaceTempView("dates_orc")
    spark.read.orc("p_lineorder.orc").createOrReplaceTempView("p_lineorder_orc")

    //query: spark.sql("...")
}
