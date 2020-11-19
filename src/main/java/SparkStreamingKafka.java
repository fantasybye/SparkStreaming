import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import pattern.TopicRegexp;
import utils.TopicCount;

public class  SparkStreamingKafka {
    public static void main(String[] args) throws InterruptedException {
        String brokers = "slave1:9092,slave2:9092,slave3:9092";
        String topics = "user_behavior";
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));

        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers) ;
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "user_behavior_test");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
                );
//
        JavaDStream<String> lines = stream.map(ConsumerRecord::value);
//        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(JSON.parse(line)).iterator());
//        JavaPairDStream<String, Integer> wordCount = words
//                .mapToPair(word -> new Tuple2<>(word, 1));

//        words.print();
        lines.foreachRDD(rdd->{
            System.out.println("rdd count"+rdd.count());
            TopicCount tc = new TopicCount();
            rdd.foreachPartition(iterator -> {
                while (iterator.hasNext()){
                    JSONObject jo = JSON.parseObject(iterator.next());
                    String content = jo.getString("content");
                    TopicRegexp tr = new TopicRegexp();
                    HashMap<String,Integer> hashMap = tr.match(content);
                    tc.addToTopicCount(hashMap);
                }
            });
            for(String key:tc.getTopicCount().keySet()) {
                System.out.println(key + ":" + tc.getTopicCount().get(key));
            }
        });
        ssc.start();
        ssc.awaitTermination();
    }
}