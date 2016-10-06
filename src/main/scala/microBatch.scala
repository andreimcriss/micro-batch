import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import java.util.TimerTask
import java.util.Timer

object microBatch{

  def main(args: Array[String]): Unit = {
   if (args.length != 2) {
      System.err.println(s"""
        |Usage: microBatch <microbatch_interval> <application_name>
        |  <microbatch_interval> is the amount of time in seconds between two succesive runs of the job
        |  <application_name> is the name of the application as it will appear in YARN. It will be prefixed with "microbatch"
                """.stripMargin)
      System.exit(1)
    }
        //read arguments
        val Array(micro_time,app_name) = args
        val batch_interval = micro_time.toInt*1000

        //intialize Spark Session
        val spark = SparkSession.builder().master("yarn").appName("microbatch"+app_name).config("spark.sql.warehouse.dir","hdfs:///user/hive/warehouse/").enableHiveSupport().getOrCreate()
        spark.conf.set("spark.executor.memory", "5g")
        spark.conf.set("spark.rdd.compress","true")

        
        //For each StreamPath, read schema and initialize stream with .readStream

        //Stream 1
        val path_1 = "hdfs:///kafka_raw_stream/generic/generic"
        val source_schema_1 = spark.read.load(path_1).schema
        val stream_session_1 = spark.readStream.schema(source_schema_1).parquet(path_1)

        //Stream 2
        val path_2 = "hdfs:///kafka_test_generic_consumer/mkt_products/schema-version-1"
        val source_schema_2 = spark.read.load(path_2).schema
        val stream_session_2 = spark.readStream.schema(source_schema_2).parquet(path_2)

        //Stream n
        //val path_n = "hdfs:///"
        //val source_schema_n = spark.read.load(path_n).schema
        //val stream_session_n = spark.readStream.schema(source_schema_n).parquet(path_n)

        //create queries for each created stream
        //For Stream 1
        val result_query_1_1 = stream_session_1.select("@table").groupBy("@table").count()
        val result_query_1_2 = stream_session_1.groupBy("furnizor").count()

        //For Stream 2
        val result_query_2_1 = stream_session_2.select("@table").groupBy("@table").count()
        val result_query_2_2 = stream_session_2.groupBy("furnizor").count()
        val result_query_2_3 = stream_session_2.groupBy("@update").count().sort(col("count").desc)

        //For Stream n
        //val result_query_n_1 = .........
        //val result_query_n_2 = .........
        //val result_query_n_n = .........

        //Write Each Query to a Memory Table
        //For Stream 1
        val write_query_1_1 = result_query_1_1.writeStream.outputMode("complete").format("memory").queryName("mem_table_1_1").start()
        val write_query_1_2 = result_query_1_2.writeStream.outputMode("complete").format("memory").queryName("mem_table_1_2").start()

        //For Stream 2
        val write_query_2_1 = result_query_2_1.writeStream.outputMode("complete").format("memory").queryName("mem_table_2_1").start()
        val write_query_2_2 = result_query_2_2.writeStream.outputMode("complete").format("memory").queryName("mem_table_2_2").start()
        val write_query_2_3 = result_query_2_3.writeStream.outputMode("complete").format("memory").queryName("mem_table_2_3").start()

        //For Stream n
        //val write_query_n_1 = result_query_n_1.writeStream.outputMode("complete").format("memory").queryName("mem_table_n_1").start()
        //val write_query_n_2 = result_query_n_2.writeStream.outputMode("complete").format("memory").queryName("mem_table_n_2").start()
        //val write_query_n_n = result_query_n_n.writeStream.outputMode("complete").format("memory").queryName("mem_table_n_n").start()

        //Spark Session can also output to console. Example below:
        //val write_query = result_query.writeStream.outputMode("complete").format("console").queryName("test_micro_sparksubmit").start()

        //Write Memory Tables to Physical SparkSQL tables periodically
        val selectall_query = "SELECT * FROM "
        val timer = new Timer("Rewrite Table", true)
        timer.schedule(new TimerTask{
                override def run() {
                        //Stream 1
                        spark.sql(selectall_query+"mem_table_1_1").write.format("parquet").mode("overwrite").saveAsTable("spark_sql_table_1_1")
                        spark.sql(selectall_query+"mem_table_1_2").write.format("parquet").mode("overwrite").saveAsTable("spark_sql_table_1_2")
                        //Stream 2
                        spark.sql(selectall_query+"mem_table_2_1").write.format("parquet").mode("overwrite").saveAsTable("spark_sql_table_2_1")
                        spark.sql(selectall_query+"mem_table_2_2").write.format("parquet").mode("overwrite").saveAsTable("spark_sql_table_2_2")
                        spark.sql(selectall_query+"mem_table_2_3").write.format("parquet").mode("overwrite").saveAsTable("spark_sql_table_2_3")

                        //Stream n
                        //spark.sql(selectall_query+"mem_table_n_1").write.format("parquet").mode("overwrite").saveAsTable("spark_sql_table_n_1")
                        //spark.sql(selectall_query+"mem_table_n_2").write.format("parquet").mode("overwrite").saveAsTable("spark_sql_table_n_2")
                        //spark.sql(selectall_query+"mem_table_n_n").write.format("parquet").mode("overwrite").saveAsTable("spark_sql_table_n_n")
                }
        }, batch_interval, batch_interval)

        //Await Termination For all queries

        //Stream 1
        write_query_1_1.awaitTermination()
        write_query_1_2.awaitTermination()
        //Stream 2
        write_query_1_1.awaitTermination()
        write_query_2_2.awaitTermination()
        write_query_2_3.awaitTermination()
        //Stream n
        //write_query_n_1.awaitTermination()
        //write_query_n_2.awaitTermination()
        //write_query_n_n.awaitTermination()
        }

}
