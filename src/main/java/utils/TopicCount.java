package utils;

import java.util.HashMap;
import java.util.Map;

public class TopicCount {
    private HashMap<String, Integer> topicCount = new HashMap<>();
    public void addToTopicCount(HashMap<String,Integer> hashMap){
        for (Map.Entry<String, Integer> entry:hashMap.entrySet()){
            if(topicCount.containsKey(entry.getKey())){
                topicCount.put(entry.getKey(), topicCount.get(entry.getKey())+ entry.getValue());
            }else{
                topicCount.put(entry.getKey(), entry.getValue());
            }
        }
    }
    public HashMap<String, Integer> getTopicCount(){
        return topicCount;
    }
}
