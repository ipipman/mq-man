package cn.ipman.mq.client.annotation;

import cn.ipman.mq.client.broker.MQListenerProcessor;
import cn.ipman.mq.client.config.MQClientBootstrapConfig;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * 启用 IpMan 消息队列客户端的注解。
 * 使用此注解将自动配置客户端启动配置和消息监听器处理器，以启用消息队列的功能。
 *
 * @Author IpMan
 * @Date 2024/7/14 08:49
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@Import({MQClientBootstrapConfig.class, MQListenerProcessor.class})
@SuppressWarnings("unused")
public @interface EnableMqMan {
}
