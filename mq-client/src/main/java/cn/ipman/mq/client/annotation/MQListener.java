package cn.ipman.mq.client.annotation;

import java.lang.annotation.*;


/**
 * 定义了一个消息队列监听器的注解，用于标记方法作为消息监听处理方法。
 * 被此注解标记的方法会在运行时被MQ客户端框架调用，以处理特定主题的消息。
 *
 * @Author IpMan
 * @Date 2024/7/14 08:49
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
@Inherited
public @interface MQListener {
    String[] topic();
}
