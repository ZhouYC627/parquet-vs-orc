import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class sparksql {

    public static String url = "jdbc:hive2://headnodehost:10002/;transportMode=http";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("ParquetTest")
                .set("spark.sql.parquet.filterPushdown", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc.sc());

        sqlContext.sql("use ssb_10");
        sqlContext.sql("show tables").show();
        //sqlContext.sql("SELECT COUNT(*) FROM p_lineorder").show();
        /*
        sqlContext.sql("select sum(v_revenue) as revenue" +
                " from p_lineorder" +
                " group by d_year");
         */
        //sqlContext.sql("select d_year,sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey group by d_year");
        Dataset<Row> df =
                sqlContext.sql("select d_date, sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey group by d_datekey");

        //df.show();
        String parquetFileName = "sum_revenue.parquet";
        df.write().parquet(parquetFileName);
        System.out.print("Saving to " + parquetFileName);


        //Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");


        sc.close();
    }



}
