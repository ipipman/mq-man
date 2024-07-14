package cn.ipman.mq.client.annotation;

import cn.ipman.mq.client.broker.MQListenerProcessor;
import cn.ipman.mq.client.config.MQClientConfig;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/7/14 08:49
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@Import({MQClientConfig.class, MQListenerProcessor.class})
@SuppressWarnings("unused")
public @interface EnableMqMan {
}
