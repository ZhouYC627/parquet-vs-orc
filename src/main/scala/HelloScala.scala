import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext



object HelloScala {
  def main(args: Array[String]): Unit = {
    println("HelloWorld!")

    val conf = new SparkConf().setAppName("JSONDataSource")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    sqlContext.sql("show tables").show()
    sqlContext.sql("select count(*) from p_lineorder")
    val df = sqlContext.sql("select d_datekey, sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey group by d_datekey")
    df.write.parquet("rev-by-date.parquet")

    //val parquetFileDF = spark.read.parquet("people.parquet")
  }
}
