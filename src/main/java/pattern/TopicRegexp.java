package pattern;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopicRegexp {
    String regex = "#[^#]*#";
    Pattern pattern = Pattern.compile(regex);
    public HashMap<String, Integer> match(String str){
        HashMap<String,Integer> result = new HashMap<>();
        Matcher matcher = pattern.matcher(str);
        while(matcher.find()){
            String key = matcher.group();
            key = key.replace("#", "");
            if(result.containsKey(key)){
                result.put(key,result.get(key)+1);
            }else{
                result.put(key,1);
            }
        }
        return result;
    }

//    public static  void main(String[] args){
//        TopicRegexp t = new TopicRegexp();
//        HashMap<String,Integer> test = new HashMap<>();
//        test.put("sdasd", 2);
//        test.put("dadas", 4);
//        HashMap<String,Integer> hashMap = t.match("#sdasd#sasdadasd#dadas#");
//        for (Map.Entry<String, Integer> entry:hashMap.entrySet()){
//            if(test.containsKey(entry.getKey())){
//                test.put(entry.getKey(), test.get(entry.getKey())+ entry.getValue());
//            }else{
//                test.put(entry.getKey(), entry.getValue());
//            }
//        }
//        for(String key:test.keySet()) {
//            System.out.println(key + ":" + test.get(key));
//        }
//    }
}
