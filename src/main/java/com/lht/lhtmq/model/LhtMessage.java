package com.lht.lhtmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * message model
 *
 * @author Leo
 * @date 2024/06/26
 */
@Data
@AllArgsConstructor
public class LhtMessage<T> {

    static AtomicLong idGen = new AtomicLong(0);

//    private String topic;
    private long id;
    private T body;
    private Map<String, String> headers;//系统属性
    private Map<String, String> properties;//业务属性


    public static long getID() {
        return idGen.getAndIncrement();
    }

    public static LhtMessage create(String body, Map<String, String> headers) {
        return new LhtMessage(getID(), body, headers, null);
    }

}
