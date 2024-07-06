package com.lht.lhtmq.demo;

import com.alibaba.fastjson.JSON;
import com.lht.lhtmq.model.LhtMessage;
import com.lht.lhtmq.client.*;
import lombok.SneakyThrows;

import java.util.HashMap;

/**
 * @author Leo
 * @date 2024/06/27
 */
public class MqDemo {

    @SneakyThrows
    public static void main(String[] args) {

        int ids = 0;

        String topic = "com.lht.test";
        LhtBroker broker = LhtBroker.getDefault();
//        broker.createTopic(topic);

        LhtProducer producer = broker.createProducer();

        LhtConsumer<?> consumer = broker.createConsumer(topic);
        consumer.listener(topic, (message) -> System.out.println("onMessage==> " + message));

//        LhtConsumer<?> consumer1 = broker.createConsumer(topic);

        for (int i = 0; i < 10000; i++) {
            Order order = new Order(ids, "items" + ids, ids * 100);
            ids++;
            producer.send(topic, LhtMessage.create(JSON.toJSONString(order), new HashMap<>()));
        }

//        for (int i = 0; i < 10; i++) {
//            LhtMessage<String> message = (LhtMessage<String>) consumer1.recv(topic);
//            System.out.println(message);//做业务处理
//            consumer1.ack(topic, message);
//        }


//        while (true) {
//            LhtMessage<Order> message = consumer.poll(1000);
//            if (message != null) {
//                System.out.println(message.getBody());
//            }
//        }

        while (true){
            char c= (char) System.in.read();
            if (c == 'q' || c == 'e') {
//                consumer1.unsubscribe(topic);
                System.out.println("====> 退出");
                break;
            }

            if (c == 'p') {
                Order order = new Order(ids, "items" + ids, ids * 100);
                ids++;
                producer.send(topic, LhtMessage.create(JSON.toJSONString(order), new HashMap<>()));
                System.out.println("====> 发送消息 " + order);
            }

            if (c == 'c') {
//                LhtMessage<Order> poll = consumer.recv(1000);
//                LhtMessage<String> poll = (LhtMessage<String>) consumer1.recv(topic);
//                if (poll != null) {
//                    System.out.println("====> 消费消息 " +  poll.getBody());
//                    consumer1.ack(topic, poll);
//                }
            }

        }


    }


}
