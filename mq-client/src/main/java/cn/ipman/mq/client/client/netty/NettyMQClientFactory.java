package cn.ipman.mq.client.client.netty;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * NettyMQClient工厂类，用于创建、管理和回收NettyMQClient实例。
 * 使用Apache Commons Pool2库实现对象池化。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQClientFactory implements PooledObjectFactory<NettyMQClient> {

    // MQ服务器的主机地址
    private final String host;

    // MQ服务器的端口号
    private final int port;

    /**
     * 构造函数，初始化MQTT服务器的主机地址和端口号。
     *
     * @param host MQTT服务器的主机地址
     * @param port MQTT服务器的端口号
     */
    public NettyMQClientFactory(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * 创建一个新的NettyMQClient实例并启动它。
     *
     * @return 已启动的NettyMQClient实例的PooledObject包装
     * @throws Exception 如果创建或启动客户端时发生错误
     */
    @Override
    public PooledObject<NettyMQClient> makeObject() throws Exception {
        NettyMQClient client = new NettyMQClient(host, port);
        client.start();
        return new DefaultPooledObject<>(client);
    }

    /**
     * 关闭并销毁一个NettyMQClient实例。
     *
     * @param p 包含待销毁的NettyMQClient实例的PooledObject
     * @throws Exception 如果关闭客户端时发生错误
     */
    @Override
    public void destroyObject(PooledObject<NettyMQClient> p) throws Exception {
        p.getObject().close();
    }


    /**
     * 验证一个NettyMQClient实例是否有效。
     *
     * @param p 包含待验证的NettyMQClient实例的PooledObject
     * @return 如果客户端实例有效则为true，否则为false
     */
    @Override
    public boolean validateObject(PooledObject<NettyMQClient> p) {
        return p.getObject().isActive();
    }

    /**
     * 激活一个NettyMQClient实例。
     * 本实现中无需激活操作。
     *
     * @param p 包含待激活的NettyMQClient实例的PooledObject
     * @throws Exception 如果激活操作失败
     */
    @Override
    public void activateObject(PooledObject<NettyMQClient> p) throws Exception {
        // No activation required
    }

    /**
     * 使一个NettyMQClient实例处于待激活状态。
     * 本实现中无需待激活操作。
     *
     * @param p 包含待处于待激活状态的NettyMQClient实例的PooledObject
     * @throws Exception 如果待激活操作失败
     */
    @Override
    public void passivateObject(PooledObject<NettyMQClient> p) throws Exception {
        // No passivation required
    }

}
