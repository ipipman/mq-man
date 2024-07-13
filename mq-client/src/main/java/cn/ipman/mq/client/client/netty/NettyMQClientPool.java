package cn.ipman.mq.client.client.netty;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQClientPool {

    private final GenericObjectPool<NettyMQClient> clientPool;

    public NettyMQClientPool(String host, int port, int maxTotal, int maxIdle, int minIdle) {
        GenericObjectPoolConfig<NettyMQClient> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);

        NettyMQClientFactory factory = new NettyMQClientFactory(host, port);
        clientPool = new GenericObjectPool<>(factory, poolConfig);
    }

    public NettyMQClient borrowClient() throws Exception {
        return clientPool.borrowObject();
    }

    public void returnClient(NettyMQClient client) {
        clientPool.returnObject(client);
    }

    public void close() {
        clientPool.close();
    }
}
