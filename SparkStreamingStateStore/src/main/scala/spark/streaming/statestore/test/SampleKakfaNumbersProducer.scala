package spark.streaming.statestore.test

import java.util.{Random,Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig,ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

// com.spark.streaming.samplecodes.producer.SampleKakfaProducer22
object SampleKakfaNumbersProducer {
  
  
  def main(args: Array[String]): Unit = {
        
    println("starting SampleKakfaProducer :--: " + args.mkString(" , "))
    
    val prop = new Properties();
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.brokers);
    
    var count = 1
    val producer = new KafkaProducer(prop, new StringSerializer, new StringSerializer)
    
    val list = List(1,2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97)
    while (true) {
      list.foreach(r=>{
        if(count%r==0){
          val productRecord = new ProducerRecord(Configs.topic,new Random().nextInt(10), "" , r.toString)
          producer.send(productRecord)
        }
      })
      count+=1
      Thread.sleep(5000);
    }
    
    producer.close
  }
}