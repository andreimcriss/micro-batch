import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.util.TimerTask
import java.util.Timer

object microBatch{

  def main(args: Array[String]): Unit = {
   if (args.length != 1) {
      System.err.println(s"""
        |Usage: microBatch <source_locations> <microbatch_interval> <queries_file> <output_tables_file>
        |  <source_location> is the HDFS path where the Parquet files are located
        |  <microbatch_interval> is the amount of time in seconds between two succesive runs of the job
                """.stripMargin)
      System.exit(1)
    }

      //.set("spark.storage.memoryFraction", "1")
      //.set("spark.streaming.unpersist", "true")

        val Array(source_location) = args

        val spark = SparkSession.builder().master("yarn").appName("microbatch").config("spark.sql.warehouse.dir","hdfs:///user/hive/warehouse/").enableHiveSupport().getOrCreate()
        spark.conf.set("spark.executor.memory", "5g")
        spark.conf.set("spark.rdd.compress","true")

        val source_schema = spark.read.load(source_location).schema
        val stream_session = spark.readStream.schema(source_schema).parquet(source_location)

        //insert query here using stream_session dataframe

        val result_query = stream_session.select("customers_city").groupBy("customers_city").count()

        //write to memory table
        val write_query = result_query.writeStream.outputMode("complete").format("memory").queryName("test_micro_sparksubmit").start()


//val write_query = result_query.writeStream.outputMode("complete").format("console").queryName("test_micro_sparksubmit").start()

        val timer = new Timer("rewrite table", true)
        timer.schedule(new TimerTask{
                override def run() {
                        spark.sql("SELECT * FROM test_micro_sparksubmit").write.format("parquet").mode("overwrite").saveAsTable("microbatch_table")
                }
        }, 30000, 15000)

        write_query.awaitTermination()
        }

}
