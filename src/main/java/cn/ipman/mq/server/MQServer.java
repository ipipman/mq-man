package cn.ipman.mq.server;

import cn.ipman.mq.core.IMMessage;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * MQ server.
 *
 * @Author IpMan
 * @Date 2024/6/30 20:17
 */
@Controller
@RequestMapping("/mq")
public class MQServer {


    // send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestParam("cid") String consumerId,
                               @RequestBody IMMessage<String> message) {
        return Result.ok();
    }

    // receive
    @RequestMapping("/receive")
    public Result<IMMessage<String>> receive(@RequestParam("t") String topic,
                                             @RequestParam("cid") String consumerId) {
        return Result.msg("hello," + consumerId);
    }

    // ack
    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                                         @RequestParam("cid") String consumerId,
                                         @RequestParam("offset") Integer offset) {
        return Result.ok();
    }

    // subscriber
    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t") String topic, @RequestParam("cid") String consumerId) {
        return Result.ok();
    }

    // unsubscribe
    @RequestMapping("/unsub")
    public Result<String> unSubscribe(@RequestParam("t") String topic, @RequestParam("cid") String consumerId) {
        return Result.ok();
    }

}
