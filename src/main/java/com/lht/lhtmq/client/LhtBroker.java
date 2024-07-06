package com.lht.lhtmq.client;

import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.lht.lhtmq.model.LhtMessage;
import com.lht.lhtmq.model.Result;
import lombok.Data;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;

/**
 * @author Leo
 * @date 2024/06/26
 */
@Data
public class LhtBroker {

    @Getter
    public static LhtBroker Default = new LhtBroker();

    public static String brokerUrl = "http:localhost:8765/lhtmq";

    static {
        init();
    }

    public static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, LhtConsumer<?>> consumers = getDefault().getConsumers();
            consumers.forEach((k,v)->{
                v.forEach(c->{
                    LhtMessage<?> recv = c.recv(k);
                    if (recv==null) return;
                    try {
                        c.getListener().onMessage(recv);
                        c.ack(k, recv);
                    }catch (Exception e){
                        // TODO 重试
                    }

                });
            });
        },100,100);
    }

    public LhtProducer createProducer(){ return new LhtProducer(this);}

    public LhtConsumer<?> createConsumer(String topic){
        LhtConsumer<?> lhtConsumer = new LhtConsumer(this);
        lhtConsumer.subscribe(topic);
        return lhtConsumer;
    }


    public boolean send(String topic, LhtMessage lhtMessage) {
        System.out.println(" ==> send topic:"+topic+" message:"+lhtMessage);
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(lhtMessage), brokerUrl + "/send?t=" + topic,new TypeReference<Result<String>>(){});
        System.out.println(" ==> send result:"+result);
        return result.getCode() == 1;

    }

    public void sub(String topic, String id) {
        System.out.println(" ==> sub topic:"+topic+" cid:"+id);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic+ "&cid=" + id,new TypeReference<Result<String>>(){});
        System.out.println(" ==> sub result:"+result);
    }

    public void unsub(String topic, String id) {
        System.out.println(" ==> sub topic:"+topic+" cid:"+id);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic+ "&cid=" + id,new TypeReference<Result<String>>(){});
        System.out.println(" ==> sub result:"+result);
    }

    public <T> LhtMessage<T> recv(String topic, String id) {
        System.out.println(" ==> recv topic:"+topic+" cid:"+id);
        Result<LhtMessage<String>> result = HttpUtils.httpGet(brokerUrl + "/recv?t=" + topic + "&cid=" + id,new TypeReference<Result<LhtMessage<String>>>(){});
        System.out.println(" ==> recv result:"+result);
        return (LhtMessage<T>)result.getData();
    }

    public <T> List<LhtMessage<String>> batch(String topic, String id, int size) {
        System.out.println(" ==> recv topic:"+topic+" cid:"+id);
        String url = brokerUrl + "/batch?t=" + topic + "&cid=" + id;
        if (size > 0) {
            url += "&size=" + size;
        }
        Result<List<LhtMessage<String>>> result = HttpUtils.httpGet(url,new TypeReference<Result<List<LhtMessage<String>>>>() {});
        System.out.println(" ==> recv result:"+result);
        return (List<LhtMessage<String>>)result.getData();
    }


    public boolean ack(String topic, String id, int offset) {
        System.out.println(" ==> ack topic:"+topic+" cid:"+id+" offset:"+offset);
        Result<Integer> result = HttpUtils.httpGet(brokerUrl + "/ack?t=" + topic + "&cid=" + id + "&offset=" + offset,new TypeReference<Result<Integer>>(){});
        System.out.println(" ==> ack result:"+result);
        return result.getData() == 1;
    }

    private MultiValueMap<String,LhtConsumer<?>> consumers = new LinkedMultiValueMap<>();

    public void addConsumer(String topic, LhtConsumer<?> consumer) {
        consumers.add(topic, consumer);
    }
}
