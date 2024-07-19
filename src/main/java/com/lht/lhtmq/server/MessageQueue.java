package com.lht.lhtmq.server;

import com.lht.lhtmq.model.LhtMessage;
import com.lht.lhtmq.store.Indexer;
import com.lht.lhtmq.store.MessageStore;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Leo
 * @date 2024/06/30
 */
public class MessageQueue {

    public static final Map<String, MessageQueue> queues = new HashMap<>();

    public static final String TEST_TOPIC = "com.lht.test";

    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
//        queues.put("a", new MessageQueue("a"));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();

    private String topic;
//    private LhtMessage<?>[] queue = new LhtMessage[10 * 1024];

    private MessageStore store = null;

    public MessageQueue(String topic){
        this.topic = topic;
        this.store= new MessageStore(topic);
        store.init();
    }

    public int send(LhtMessage<String> message) {
        int offset = store.pos();
        message.getHeaders().put("X-offset", String.valueOf(offset));//记录偏移量
        store.write(message);
        return offset;
    }

    public LhtMessage<?> recv(int offset) {
        return store.read(offset);
    }

    public void subscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }

    public void unsubscribe(MessageSubscription subscription) {
        String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> sub: " + subscription);
        if (messageQueue==null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> unsub: " + subscription);
        if (messageQueue==null) return;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, LhtMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        System.out.println(" ===>> send: topic/message :" + topic + "/" + message);
        if (messageQueue==null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    // 使用此方法，需要手工调用ack，更新订阅关系里的offset
    public static LhtMessage<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue==null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int next_offset = 0;
            if (offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                next_offset = offset + entry.getLength();
            }
            LhtMessage<?> recv = messageQueue.recv(next_offset);
            System.out.println(" ======>> recv: topic/cid/ind = " + topic + "/" + consumerId + "/" + next_offset);
            System.out.println(" ======>> message: " + recv);
            return recv;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    public static List<LhtMessage<?>> batchRecv(String topic, String consumerId, int size) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue==null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int next_offset = 0;
            if (offset > -1) {
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                next_offset = offset + entry.getLength();
            }
            List<LhtMessage<?>> result = new ArrayList<>();
            LhtMessage<?> recv = messageQueue.recv(next_offset);
            while (recv != null) {
                result.add(recv);
                if (next_offset>size) break;
                Indexer.Entry entry = Indexer.getEntry(topic, offset);
                next_offset = offset + entry.getLength();
                recv=messageQueue.recv(next_offset);
            }
            System.out.println(" ======>> recv: topic/cid/size = " + topic + "/" + consumerId + "/" + result.size());
            System.out.println(" ======>> message: " + recv);
            return result;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    public static LhtMessage<?> recv(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.recv(offset + 1);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            MessageSubscription messageSubscription = messageQueue.subscriptions.get(consumerId);
            //大于当前offset，并且小于等于当前消息下标才有效
            if (offset > messageSubscription.getOffset() && offset < MessageStore.LEN) {
                System.out.println(" ======>> ack: topic/cid/offset = " + topic + "/" + consumerId + "/" + offset);
                messageSubscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

}
