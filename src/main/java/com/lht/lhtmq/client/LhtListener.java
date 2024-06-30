package com.lht.lhtmq.client;

import com.lht.lhtmq.model.LhtMessage;

/**
 * @author Leo
 * @date 2024/06/27
 */
public interface LhtListener<T>{

    void onMessage(LhtMessage<T> message);

}
