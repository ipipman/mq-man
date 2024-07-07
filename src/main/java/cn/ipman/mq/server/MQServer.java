package cn.ipman.mq.server;

import cn.ipman.mq.broker.MQProducer;
import cn.ipman.mq.demo.Order;
import cn.ipman.mq.model.Message;
import cn.ipman.mq.model.HttpResult;
import cn.ipman.mq.model.Statistical;
import cn.ipman.mq.model.Subscription;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
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
    public HttpResult<String> send(@RequestParam("t") String topic,
                                   @RequestBody Message<String> message) {
        return HttpResult.ok("msg" + MessageQueue.send(topic, message));
    }


    // receive
    @RequestMapping("/receive")
    public HttpResult<Message<?>> receive(@RequestParam("t") String topic,
                                          @RequestParam("cid") String consumerId) {
        return HttpResult.msg(MessageQueue.receive(topic, consumerId));
    }


    // receive
    @RequestMapping("/batch-receive")
    public HttpResult<List<Message<?>>> batchReceive(@RequestParam("t") String topic,
                                                     @RequestParam("cid") String consumerId,
                                                     @RequestParam(name = "size", required = false, defaultValue = "1000") int size) {
        return HttpResult.msg(MessageQueue.batchReceive(topic, consumerId, size));
    }


    // ack
    @RequestMapping("/ack")
    public HttpResult<String> ack(@RequestParam("t") String topic,
                                  @RequestParam("cid") String consumerId,
                                  @RequestParam("offset") Integer offset) {
        return HttpResult.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

    // 1. subscriber
    @RequestMapping("/sub")
    public HttpResult<String> subscribe(@RequestParam("t") String topic,
                                        @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new Subscription(topic, consumerId, -1));
        return HttpResult.ok();
    }

    // unsubscribe
    @RequestMapping("/unsub")
    public HttpResult<String> unSubscribe(@RequestParam("t") String topic,
                                          @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(new Subscription(topic, consumerId, -1));
        return HttpResult.ok();
    }

    // stat
    @RequestMapping("/stat")
    public HttpResult<Statistical> stat(@RequestParam("t") String topic,
                                        @RequestParam("cid") String consumerId) {
        return HttpResult.stat(MessageQueue.stat(topic, consumerId));
    }


    @Autowired
    private MQProducer producer;

    // test product
    @RequestMapping("/test-send")
    public HttpResult<String> testSend(){
        Order order = new Order(1, "item" + 1, 100);
        producer.send("cn.ipman.test", new Message<>(1, JSON.toJSONString(order), null));
        return HttpResult.ok();
    }


}
