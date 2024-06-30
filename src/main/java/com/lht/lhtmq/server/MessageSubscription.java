package com.lht.lhtmq.server;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Leo
 * @date 2024/06/30
 */
@Data
@AllArgsConstructor
public class MessageSubscription {

    private String topic;
    private String consumerId;
    private int offset = -1;


}
