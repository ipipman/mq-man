package cn.ipman.mq.server;

import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.Result;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * MQ server.
 *
 * @Author IpMan
 * @Date 2024/6/30 20:17
 */
@RestController
@RequestMapping("/mq")
public class MQServer {

    // send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestBody Message<String> message) {
        return Result.ok("msg" + MessageQueue.send(topic, message));
    }

    // receive
    @RequestMapping("/receive")
    public Result<Message<?>> receive(@RequestParam("t") String topic,
                                      @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.receive(topic, consumerId));
    }


    // receive
    @RequestMapping("/batch-receive")
    public Result<List<Message<?>>> batchReceive(@RequestParam("t") String topic,
                                     @RequestParam("cid") String consumerId,
                                     @RequestParam(name = "size", required = false, defaultValue = "1000") int size) {
        return Result.msg(MessageQueue.batchReceive(topic, consumerId, size));
    }


    // ack
    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offset) {
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

    // 1. subscriber
    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t") String topic,
                                    @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

    // unsubscribe
    @RequestMapping("/unsub")
    public Result<String> unSubscribe(@RequestParam("t") String topic,
                                      @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

}
