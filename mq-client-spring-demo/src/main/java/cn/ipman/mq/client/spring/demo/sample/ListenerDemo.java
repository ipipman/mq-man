package cn.ipman.mq.client.spring.demo.sample;

import cn.ipman.mq.client.annotation.MQListener;
import cn.ipman.mq.metadata.model.Message;
import org.springframework.stereotype.Component;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/7/14 08:52
 */
@Component
public class ListenerDemo {

    @MQListener(topic = {"im.order", "cn.ipman.test"})
    public void demo(Message<?> msg) {
        System.out.println("........." + msg);
    }

}
