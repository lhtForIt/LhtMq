package com.lht.lhtmq.server;

import com.lht.lhtmq.model.LhtMessage;
import com.lht.lhtmq.model.Result;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Leo
 * @date 2024/06/30
 */
@RestController
@RequestMapping("/lhtmq")
public class MqServer {

    // send
    @RequestMapping("/send")
    public Result send(@RequestParam("t") String topic,
                       @RequestBody LhtMessage<String> message) {
        return Result.ok("" + MessageQueue.send(topic, message));
    }

    // recv
    @RequestMapping("/recv")
    public Result recv(@RequestParam("t") String topic,
                       @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }

    @RequestMapping("/batch")
    public Result batch(@RequestParam("t") String topic,
                       @RequestParam("cid") String consumerId,
                       @RequestParam(name = "size", defaultValue = "1000", required = false) int size) {
        return Result.msg(MessageQueue.batchRecv(topic, consumerId, size));
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
    public Result sub(@RequestParam("t") String topic, @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId,-1));
        return Result.ok();
    }

    // unsub
    @RequestMapping("/unsub")
    public Result unsub(@RequestParam("t") String topic,@RequestParam("cid") String consumerId){
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }





}
