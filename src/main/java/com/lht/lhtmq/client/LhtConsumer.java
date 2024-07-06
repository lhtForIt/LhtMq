package com.lht.lhtmq.client;

import com.lht.lhtmq.model.LhtMessage;
import lombok.Data;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Leo
 * @date 2024/06/26
 */
@Data
public class LhtConsumer<T> {

    private String id;
    LhtBroker broker;

    static AtomicInteger idGen = new AtomicInteger(0);

    public LhtConsumer(LhtBroker broker) {
        this.broker = broker;
        this.id = "CID" + idGen.getAndIncrement();
    }

    public void subscribe(String topic){
        broker.sub(topic, id);
    }
    public void unsubscribe(String topic){
        broker.unsub(topic, id);
    }
    public LhtMessage<T> recv(String topic){
        return broker.recv(topic, id);
    }

    public List<LhtMessage<String>> batch(String topic, int size) {
        return broker.batch(topic, id, size);
    }

    public boolean ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }
    public boolean ack(String topic, LhtMessage<?> message) {
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }

    public void listener(String topic, LhtListener listener) {
        this.listener = listener;
        broker.addConsumer(topic, this);
    }

    private LhtListener listener;


}
