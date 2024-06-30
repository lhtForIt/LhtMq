package com.lht.lhtmq.client;

import com.lht.lhtmq.model.LhtMessage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Leo
 * @date 2024/06/26
 */
public class LhtConsumer<T> {

    private String id;
    LhtBroker broker;
    String topic;
    LhtMq mq;

    static AtomicInteger idGen = new AtomicInteger(0);

    public LhtConsumer(LhtBroker broker) {
        this.broker = broker;
        this.id = "CID" + idGen.getAndIncrement();
    }

    public void subscribe(String topic){
        this.topic = topic;
        mq = broker.find(topic);
        if(mq==null) throw new RuntimeException("topic not found");
    }
    public LhtMessage<T> poll(long timeout){
        return mq.poll(timeout);
    }

    public void listener(LhtListener listener){
        mq.addListener(listener);
    }




}
