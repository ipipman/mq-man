package cn.ipman.mq.client.broker;

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
import cn.ipman.mq.client.annotation.MQListener;

import java.lang.reflect.Method;
import java.util.Set;

/**
 * MQ监听器处理器，用于扫描并注册MQ监听器方法。
 * 该类实现了BeanPostProcessor接口，以便在Spring Bean初始化后对其进行处理。
 * 同时实现了ApplicationContextAware和EnvironmentAware接口，以获取Spring应用上下文和环境变量。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
@Configuration
@Data
public class MQListenerProcessor implements BeanPostProcessor, ApplicationContextAware, EnvironmentAware {

    /**
     * Spring应用上下文，用于获取Bean实例。
     */
    private ApplicationContext applicationContext;

    /**
     * Spring环境变量，用于读取配置属性。
     */
    private Environment environment;

    /**
     * 在Bean初始化后进行处理。
     * 如果当前应用是测试环境（服务器端口为8765），则直接返回Bean，不进行后续处理。
     * 否则，扫描指定Bean的所有方法，寻找标有MQListener注解的方法，并注册这些监听器方法。
     *
     * @param bean     刚被初始化的Bean实例。
     * @param beanName 刚被初始化的Bean的名称。
     * @return 处理后的Bean实例。
     * @throws BeansException 如果处理过程中出现异常。
     */
    @Override
    public Object postProcessAfterInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
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
