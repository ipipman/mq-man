package cn.ipman.mq.client.demo;

import cn.ipman.mq.client.annotation.MQListener;
import cn.ipman.mq.metadata.model.Message;
import org.springframework.stereotype.Component;



/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@Component
public class SpringAnnotationListenerDemo {

    @MQListener(topic = {"im.order", "cn.ipman.test"})
    public void demo(Message<?> msg) {
        System.out.println("........." + msg);
    }

}
