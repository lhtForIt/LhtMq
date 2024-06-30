package com.lht.lhtmq.demo;

import com.lht.lhtmq.model.LhtMessage;
import com.lht.lhtmq.client.*;
import lombok.SneakyThrows;

/**
 * @author Leo
 * @date 2024/06/27
 */
public class MqDemo {

    @SneakyThrows
    public static void main(String[] args) {

        int ids = 0;

        String topic = "lht.order";
        LhtBroker broker = new LhtBroker();
        broker.createTopic(topic);

        LhtProducer producer = new LhtProducer(broker);
        LhtConsumer<Order> consumer = new LhtConsumer<>(broker);
        consumer.subscribe(topic);

        consumer.listener((message) -> System.out.println("onMessage==> " + message));

        for (int i = 0; i < 10; i++) {
            Order order = new Order(ids, "items" + ids, ids * 100);
            producer.send(topic, new LhtMessage<>(ids++, order, null, null));
        }


//        while (true) {
//            LhtMessage<Order> message = consumer.poll(1000);
//            if (message != null) {
//                System.out.println(message.getBody());
//            }
//        }

        while (true){
            char c= (char) System.in.read();
            if (c == 'q' || c == 'e') {
                System.out.println("====> 退出");
                break;
            }

            if (c == 'p') {
                Order order = new Order(ids, "items" + ids, ids * 100);
                producer.send(topic, new LhtMessage<>(ids++, order, null, null));
                System.out.println("====> 发送消息 " + order);
            }

            if (c == 'c') {
                LhtMessage<Order> poll = consumer.poll(1000);
                if (poll != null) {
                    System.out.println("====> 消费消息 " + poll.getBody());
                }
            }

        }


    }


}
