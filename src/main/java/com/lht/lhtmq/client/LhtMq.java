package com.lht.lhtmq.client;

import com.lht.lhtmq.model.LhtMessage;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Leo
 * @date 2024/06/26
 */
@AllArgsConstructor
public class LhtMq {

//    private String topic;
//    private LinkedBlockingQueue queue = new LinkedBlockingQueue();
//    private List<LhtListener> listeners = new ArrayList<>();
//
//    public LhtMq(String topic) {
//        this.topic = topic;
//    }
//
//    public boolean send(LhtMessage message) {
//        boolean offer = queue.offer(message);
//        listeners.forEach(listener -> listener.onMessage(message));
//        return offer;
//    }
//
//    //拉模式获取消息
//    @SneakyThrows
//    public <T> LhtMessage<T> poll(long timeout){
//        return (LhtMessage<T>) queue.poll(timeout, java.util.concurrent.TimeUnit.MILLISECONDS);
//    }
//
//    public void addListener(LhtListener listener) {
//        listeners.add(listener);
//    }
}
