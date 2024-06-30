package com.lht.lhtmq.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Leo
 * @date 2024/06/26
 */
public class LhtBroker {

    Map<String, LhtMq> mqMap = new ConcurrentHashMap<>(64);

    public LhtMq find(String topic) {
        return mqMap.get(topic);
    }

    public LhtMq createTopic(String topic){
        return mqMap.putIfAbsent(topic,new LhtMq(topic));
    }


}
