package cn.ipman.mq.client.client.netty;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * NettyMQClient的连接池管理类。
 * 通过Apache Commons Pool2库实现NettyMQClient的池化，以便复用客户端连接，提高性能和资源利用率。
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQClientPool {

    /**
     * 客户端连接池，用于存储和管理NettyMQClient实例。
     */
    private final GenericObjectPool<NettyMQClient> clientPool;

    /**
     * 创建NettyMQClient连接池。
     *
     * @param host     服务器主机地址。
     * @param port     服务器端口。
     * @param maxTotal 连接池允许的最大总数，包括活动和非活动连接。
     * @param maxIdle  连接池允许的最大空闲连接数。
     * @param minIdle  连接池维护的最小空闲连接数。
     */
    public NettyMQClientPool(String host, int port, int maxTotal, int maxIdle, int minIdle) {
        GenericObjectPoolConfig<NettyMQClient> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);

        NettyMQClientFactory factory = new NettyMQClientFactory(host, port);
        clientPool = new GenericObjectPool<>(factory, poolConfig);
    }

    /**
     * 从连接池借用一个NettyMQClient实例。
     * 如果池中没有可用的客户端，此方法将尝试创建一个新的客户端。
     *
     * @return 一个可用的NettyMQClient实例。
     * @throws Exception 如果无法获取客户端实例，抛出异常。
     */
    public NettyMQClient borrowClient() throws Exception {
        return clientPool.borrowObject();
    }

    /**
     * 将NettyMQClient实例返回到连接池。
     * 这个方法用于在使用完客户端后将其回收，以便其他线程可以重用。
     *
     * @param client 要返回到池中的NettyMQClient实例。
     */
    public void returnClient(NettyMQClient client) {
        clientPool.returnObject(client);
    }

    /**
     * 关闭连接池，并释放所有资源。
     * 此方法应该在不再需要连接池时调用，以确保所有资源得到正确释放。
     */
    public void close() {
        clientPool.close();
    }
}
