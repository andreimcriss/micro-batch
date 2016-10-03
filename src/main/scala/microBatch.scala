import sqlContext.implicits._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf


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
   
     //Create SparkContext
    val conf = new SparkConf()
      .setMaster("yarn-client")
      .setAppName("MicroBatch")
      .set("spark.executor.memory", "5g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.streaming.unpersist", "true")

    val Array(source_location,interval) = args


    val sparkConf = new SparkConf().setAppName("MicroBatch")
    val sc = new SparkContext(sparkConf)
	
	val ssc = SparkSession.builder().appName("microbatch").config("spark.sql.warehouse.dir","hdfs:///user/hive/warehouse/").enableHiveSupport().getOrCreate()
	//SparkSQL Hive Context
	
    
	val source_schema = spark.read.load(source_location).schema
	val stream_session = ssc.readStream.schema(source_schema).parquet(source_location)
	
	//insert query here using stream_session dataframe

	val result_query = stream_session.select("customers_city").groupBy("customers_city").count()

	//write to Hive
	val write_query = result_query.writeStream.outputMode("complete").format("memory").queryName("test_micro_sparksubmit").start()

	write_query.awaitTermination()


	}

}
