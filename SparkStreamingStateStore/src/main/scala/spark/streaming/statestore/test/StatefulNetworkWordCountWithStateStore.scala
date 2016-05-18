package spark.streaming.statestore.test

import scala.collection.mutable.ListBuffer

import org.apache.spark._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.StringDecoder
import spark.streaming.statestore.test.StateStoreUtils._

// spark.streaming.statestore.test.StatefulNetworkWordCountWithStateStore
object StatefulNetworkWordCountWithStateStore{
  def main(args: Array[String]) {

    val topicsSet = Configs.topic.split(",").toSet
    val kafkaParams = Map[String, String]("bootstrap.servers" -> Configs.brokers, "group.id" -> "StatefulNetworkWordCount")
    
    val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val sqlContet = new SQLContext(ssc.sparkContext)
        
    val keySchema = StructType(Seq(StructField("key", StringType, true)))
    val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))
    
    val stateStoreCheckpointPath = "/data/spark/stateStoreCheckpoints/"
    
    var stateStoreVersion:Long = 0
    
    val stateStoreWordCount = (store: StateStore, iter: Iterator[String]) => {
      val out = new ListBuffer[(String, Int)]
      iter.foreach { s =>
        val current = store.get(stringToRow(s)).map(rowToInt).getOrElse(0) + 1
        store.put(stringToRow(s), intToRow(current))
        out.append((s,current))
      }

      store.commit
      out.iterator
    }
    
    val opId = 100
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      .flatMap(r=>{r._2.split(" ")})
      .foreachRDD((rdd, time) =>{
        rdd
        .map(r=>(r,null))
        .partitionBy(new HashPartitioner(20))
        .map(r=>r._1)
        .mapPartitionsWithStateStore(sqlContet, stateStoreCheckpointPath, opId, storeVersion = stateStoreVersion, keySchema, valueSchema)(stateStoreWordCount)
        .collect foreach(r=> println(time  + " - " + r))
        stateStoreVersion+=1
        println(time + " batch finished")
        }
      )
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}
