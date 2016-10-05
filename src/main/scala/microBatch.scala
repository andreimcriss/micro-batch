import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.util.TimerTask
import java.util.Timer
import scala.util.control.NonFatal
import scala.collection.mutable.ArrayBuffer

object microBatch{

  def main(args: Array[String]): Unit = {
   if (args.length != 2) {
      System.err.println(s"""
        |Usage: microBatch <microbatch_interval> <input_file>
        |  <microbatch_interval> is the amount of time in seconds between two succesive runs of the job
        |  <input_file> File in JSON format. Should contain on each line:  source_stream_path, stream_type(parquet or json), query, output_table
                """.stripMargin)
      System.exit(1)
    }

        val Array(batchinterval, input_file) = args
        val batch_time = batchinterval.toInt*1000

        val spark = SparkSession.builder().master("yarn").appName("microbatch").config("spark.sql.warehouse.dir","hdfs:///user/hive/warehouse/").enableHiveSupport().getOrCreate()
        spark.conf.set("spark.executor.memory", "5g")
        spark.conf.set("spark.rdd.compress","true")

        //read input File
          val raw_input_data = spark.read.json(input_file)
        //test if there are any corrupt records and eliminate them
          //if (raw_input_data.columns.contains("_corrupt_record")) {
            try {
            val clean_input_data = raw_input_data.filter("_corrupt_record is null").select("output_table","source_stream_path","stream_type","select","where","filter","groupBy","agg","count")
          } case {
            Non NonFatal(t) => val clean_input_data = raw_input_data
          }
        //test if there are any records remaining, if not kill program
          if (!clean_input_data.columns.contains("output_table")) {
            Console.println("No valid data is present in the input file: "+input_file)
            exit()
          }

        // make a DataSet with all the paths and get the number:
          val all_paths = clean_input_data.select("source_stream_path").dropDuplicates()
          val nb_of_paths = all_paths.count
        // create empty structures for the stream session and result_queries  
          val stream_session_array = new ArrayBuffer[(org.apache.spark.sql.DataFrame,String)]()
          val result_queries_array = new ArrayBuffer[org.apache.spark.sql.DataFrame,String]()
          val streaming_queries_array = new ArrayBuffer[org.apache.spark.sql.streaming.StreamingQuery]()
        //loop through the paths DataSet
        all_paths.collect().foreach(input_path => {
                                                    //load schema
                                                    val source_schema = spark.read.load(input_path.mkString).schema
                                                    //read source_format
                                                    val source_format = clean_input_data.filter(col("source_stream_path").like(input_path.mkString)).limit(1).select("stream_type").collect().mkString.stripPrefix("[").stripSuffix("]").trim
                                                    //readStream depending on format
                                                    if(source_format == "parquet") {
                                                      stream_session_array.append((spark.readStream.schema(source_schema).parquet(input_path.mkString),input_path.mkString))
                                                    }
                                                    if(source_format == "json") {
                                                      stream_session_array.append((spark.readStream.json(input_path.mkString),input_path.mkString))
                                                    }
                                                    })
        //take each stream and launch the queries
        stream_session_array.foreach(current_stream => {
                                                          //use the location path to filter initial json
                                                          val queries_tables = clean_input_data.filter(col("source_stream_path").like(current_stream._2)).select("output_table","select","where","filter","groupBy","agg","count").dropDuplicates
                                                          //interate over values
                                                          queries_tables.collect().foreach(line => {

                                                                    if(line(4) == "") {
                                                                      if(line(6) == "0") {
                                                                        result_queries_array.append((current_stream._1.select(line(1)).where(line(2)).filter(line(3)),line(0))) 
                                                                      } else {
                                                                        result_queries_array.append((current_stream._1.select(line(1)).where(line(2)).filter(line(3).count()),line(0)))
                                                                      }
                                                                    } else {
                                                                      if(line(5) == "0") {
                                                                        result_queries_array.append((current_stream._1.select(line(1)).where(line(2)).filter(line(3).groupBy(line(4)).count()),line(0)))
                                                                      } else {
                                                                        result_queries_array.append((current_stream._1.select(line(1)).where(line(2)).filter(line(3).groupBy(line(4)).agg(line(5))),line(0)))
                                                                      }
                                                                    }

                                                          })
                                                         

                                                    
        })
      result_queries_array.foreach(current_query => { 
                                                        //start streams
                                                        streaming_queries_array.append(current_query._1.writeStream.outputMode("complete").format("memory").queryName(current_query._2+"_memory").start())

        })

      val timer = new Timer("Rewrite Tables", true)
      timer.schedule(new TimerTask{
                override def run() {
                        result_queries_array.foreach(current_query => {
                        spark.sql("SELECT * FROM "+current_query._2+"_memory").write.format("parquet").mode("overwrite").saveAsTable(current_query._2)
                
                })
                }
        }, batch_time, batch_time)
        


//This part can be used to integrate manually other streams for more complicated queries
    //    val source_schema = spark.read.load(source_location).schema
    //    val stream_session = spark.readStream.schema(source_schema).parquet(source_location)

        //insert query here using stream_session dataframe

    //    val result_query = stream_session.select("customers_city").groupBy("customers_city").count()

        //write to memory table
    //    val write_query = result_query.writeStream.outputMode("complete").format("memory").queryName("test_micro_sparksubmit").start()


//val write_query = result_query.writeStream.outputMode("complete").format("console").queryName("test_micro_sparksubmit").start()

      //  val timer = new Timer("rewrite table", true)
      //  timer.schedule(new TimerTask{
      //          override def run() {
      //                  spark.sql("SELECT * FROM test_micro_sparksubmit").write.format("parquet").mode("overwrite").saveAsTable("microbatch_table")
      //          }
      //  }, 30000, 15000)

      //  write_query.awaitTermination()
        }

}
