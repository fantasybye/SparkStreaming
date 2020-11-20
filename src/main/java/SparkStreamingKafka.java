import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class  SparkStreamingKafka {
//    private static final String CHECK_POINT_DIR = "D:\\updatestatebykey";
    private static final String CHECK_POINT_DIR = "hdfs://mycluster/weibo/checkdir";
    private static final int CHECK_POINT_DURATION_SECONDS = 10;
    private static final String brokers = "slave1:9092,slave2:9092,slave3:9092";
    //        String topics = "user_behavior";
    private static final String topics = "weibo_content";
    private static final String send_topics = "weibo_topic";

    public static void main(String[] args) throws InterruptedException {
//        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("test");
//        SparkConf conf = new SparkConf().setMaster("yarn").setAppName("SparkStreaming");
        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkStreaming");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(CHECK_POINT_DURATION_SECONDS));
        ssc.checkpoint(CHECK_POINT_DIR);
        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("metadata.broker.list", brokers) ;
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "weibo_topic");
//        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
                );
        //获得kafka流数据
        JavaDStream<String> lines = stream.map(ConsumerRecord::value);
        //获取微博内容
        JavaDStream<String> contents = lines.flatMap(line-> {
            JSONObject jo = JSON.parseObject(line);
            String content = "";
            if (jo != null) {
               content = jo.getString("content");
            }
            return Collections.singletonList(content).iterator();
        });
        //以下是实时流处理话题统计
        //正则提取微博话题
        JavaDStream<String> themes = contents.flatMap(content -> {
            Matcher matcher = Pattern.compile("#[^#]*#").matcher(content);
            List<String> themeList = new ArrayList<>();
            while(matcher.find()){
                String str = matcher.group().replace("#","");
                if(str.equals("考研")) continue;
                themeList.add(str);
            }
            return themeList.iterator();
        });
        //实时词频统计
        JavaPairDStream<String, Integer> pairs = themes.mapToPair(theme -> new Tuple2<>(theme, 1));
        //累计词频统计
        JavaPairDStream<String, Integer> counts = pairs.updateStateByKey((valueList, oldState) -> {
            Integer newState = 0;
            // 如果oldState之前已经存在，那么这个key可能之前已经被统计过，否则说明这个key第一次出现
            if (oldState.isPresent())
            {
                newState = oldState.get();
            }
            // 更新state
            for (Integer value : valueList)
            {
                newState += value;
            }
            return Optional.of(newState);
        });
        //处理结果格式
        JavaDStream<String> results = counts
                .map(tuple-> tuple._1+":"+tuple._2)
                .reduce((str1,str2)->(str1+","+str2))
                .filter(str-> !StringUtils.isEmpty(str));
        //将实时流处理结果发送到kafka
        results.foreachRDD(rdd->{
            rdd.foreach(str -> {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put("bootstrap.servers", brokers);
                properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
                ProducerRecord<String, String> message = new ProducerRecord<String, String>(send_topics,str);
                System.out.println(message.value());
                producer.send(message);
            });
        });

        //以下是学校和专业热度
        ssc.start();
        ssc.awaitTermination();
    }
}