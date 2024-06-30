package com.lht.lhtmq.server;

import com.lht.lhtmq.model.LhtMessage;
import com.lht.lhtmq.model.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Leo
 * @date 2024/06/30
 */
@Controller
@RequestMapping("/lhtmq")
public class MqServer {

    // send
    @RequestMapping("/send")
    public Result send(@RequestParam("t") String topic,
                       @RequestParam("cid") String consumerId,
                       @RequestBody LhtMessage<String> message) {
        return Result.ok("" + MessageQueue.send(topic, consumerId, message));
    }

    // recv
    @RequestMapping("/recv")
    public Result recv(@RequestParam("t") String topic,
                       @RequestParam("cid") String consumerId) {
        return Result.msg("" + MessageQueue.recv(topic, consumerId));
    }

    // ack
    @RequestMapping("/ack")
    public Result ack(@RequestParam("t") String topic,
                       @RequestParam("cid") String consumerId,
                       @RequestParam("offset") int offset) {
        return Result.ok(""+MessageQueue.ack(topic, consumerId, offset));
    }

    // sub
    @RequestMapping("/sub")
    public Result subscribe(@RequestParam("t") String topic, @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId,-1));
        return Result.ok();
    }

    // unsub
    @RequestMapping("/unsub")
    public Result unsubscribe(@RequestParam("t") String topic,@RequestParam("cid") String consumerId){
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }





}
