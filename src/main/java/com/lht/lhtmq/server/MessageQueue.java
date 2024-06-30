package com.lht.lhtmq.server;

import com.lht.lhtmq.model.LhtMessage;

import java.util.HashMap;
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
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();

    private String topic;
    private LhtMessage<?>[] queue = new LhtMessage[10 * 1024];

    //当前消息下标
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public int send(LhtMessage<?> message) {
        if (index >= queue.length) {
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public LhtMessage<?> recv(int current) {
        if (current <= index) return queue[current];
        return null;
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
        if (messageQueue==null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue==null) return;
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, String consumerId, LhtMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue==null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    // 使用此方法，需要手工调用ack，更新订阅关系里的offset
    public static LhtMessage<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue==null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int ind = messageQueue.subscriptions.get(consumerId).getOffset();
            return messageQueue.recv(ind);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    public static LhtMessage<?> recv(String topic, String consumerId, int ind) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.recv(ind);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    public static int ack(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            MessageSubscription messageSubscription = messageQueue.subscriptions.get(consumerId);
            //大于当前offset，并且小于等于当前消息下标才有效
            if (offset > messageSubscription.getOffset() && offset <= messageQueue.index) {
                messageSubscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

}
