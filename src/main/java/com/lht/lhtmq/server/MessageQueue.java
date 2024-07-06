package com.lht.lhtmq.server;

import com.lht.lhtmq.model.LhtMessage;

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
        queues.put("a", new MessageQueue("a"));
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
        message.getHeaders().put("X-offset", String.valueOf(index));//记录偏移量
        queue[index++] = message;
        return index;
    }

    public LhtMessage<?> recv(int current) {
        if (current <= index) return queue[current];
        return null;
    }

    public List<LhtMessage<?>> batch(int current, int size) {
        List<LhtMessage<?>> list = new ArrayList<>();
        if (current + size <= index || current <= index) {
            for (int i = current; i <= current + size; i++) {
                list.add(queue[i]);
            }
        }
        return list;
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
            int ind = messageQueue.subscriptions.get(consumerId).getOffset();
            LhtMessage<?> recv = messageQueue.recv(ind + 1);
            System.out.println(" ======>> recv: topic/cid/ind = " + topic + "/" + consumerId + "/" + ind);
            System.out.println(" ======>> message: " + recv);
            return recv;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    public static List<LhtMessage<?>> batchRecv(String topic, String consumerId, int size) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue==null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int ind = messageQueue.subscriptions.get(consumerId).getOffset();
            List<LhtMessage<?>> recvs = messageQueue.batch(ind, size);
            System.out.println(" ======>> recv: topic/cid/size = " + topic + "/" + consumerId + "/" + recvs.size());
            System.out.println(" ======>> message: " + recvs);
            return recvs;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

    public static LhtMessage<?> recv(String topic, String consumerId, int ind) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.recv(ind + 1);
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
                System.out.println(" ======>> ack: topic/cid/offset = " + topic + "/" + consumerId + "/" + offset);
                messageSubscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId = " + topic + "/" + consumerId);
    }

}
