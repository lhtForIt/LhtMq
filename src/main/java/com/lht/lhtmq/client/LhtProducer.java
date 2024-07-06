package com.lht.lhtmq.client;

import com.lht.lhtmq.model.LhtMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Leo
 * @date 2024/06/26
 */
@Data
@AllArgsConstructor
public class LhtProducer {

    LhtBroker broker;

    public boolean send(String topic, LhtMessage lhtMessage) {
        return broker.send(topic, lhtMessage);
    }

}
