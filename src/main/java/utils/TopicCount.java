package utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TopicCount  implements Serializable {
//    private HashMap<String, Integer> topicCount = new HashMap<>();
    public HashMap<String, Integer> addToTopicCount(HashMap<String, Integer> topicCount, HashMap<String,Integer> hashMap){
//        HashMap<String, Integer> topicCount = new HashMap<>();
        for (Map.Entry<String, Integer> entry:hashMap.entrySet()){
            if(topicCount.containsKey(entry.getKey())){
                topicCount.put(entry.getKey(), topicCount.get(entry.getKey())+ entry.getValue());
            }else{
                topicCount.put(entry.getKey(), entry.getValue());
            }
        }
        return topicCount;
    }
//    public HashMap<String, Integer> getTopicCount(){
//        return topicCount;
//    }
    public static void main(String[] args){
        TopicCount tc = new TopicCount();
        HashMap<String,Integer> test = new HashMap<>();
        HashMap<String,Integer> test2 = new HashMap<>();
        HashMap<String,Integer> output = new HashMap<>();
        test.put("sdasd", 1);
        test.put("dadas", 1);
        test2.put("sdasd", 2);
        test2.put("dadas", 3);
        output = tc.addToTopicCount(output, test);
        for(String key:output.keySet()) {
            System.out.println(key + ":" + output.get(key));
        }
        output = tc.addToTopicCount(output, test2);
        for(String key:output.keySet()) {
            System.out.println(key + ":" + output.get(key));
        }
    }
}
