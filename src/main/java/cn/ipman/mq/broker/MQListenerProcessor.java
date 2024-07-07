package cn.ipman.mq.broker;

import cn.ipman.mq.annotation.MQListener;
import lombok.Data;
import lombok.NonNull;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Set;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@Configuration
@Data
public class MQListenerProcessor implements BeanPostProcessor, ApplicationContextAware, EnvironmentAware {

    private ApplicationContext applicationContext;
    private Environment environment;

    @Override
    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
        // 为了方便测试...
        if ("8765".equals(environment.getProperty("server.port"))) {
            return bean;
        }
        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
        Set<Method> methods = MethodIntrospector.selectMethods(targetClass,
                (ReflectionUtils.MethodFilter) method -> AnnotatedElementUtils.hasAnnotation(method, MQListener.class));

        // 查找消费者程序
        methods.forEach(method -> {
            MQListener listener = method.getAnnotation(MQListener.class);
            MQListenerEndpoint endpoint = new MQListenerEndpoint();
            endpoint.setBean(bean);
            endpoint.setMethod(method);
            endpoint.setTopic(listener.topic());

            MQListenerContainerFactory factory = applicationContext.getBean(MQListenerContainerFactory.class);
            factory.registryListener(endpoint);
        });
        return bean;
    }
}
