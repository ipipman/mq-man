package cn.ipman.mq.client.netty;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * Description for this class
 *
 * @Author IpMan
 * @Date 2024/6/29 20:07
 */
public class NettyMQClientFactory implements PooledObjectFactory<NettyMQClient> {

    private final String host;
    private final int port;

    public NettyMQClientFactory(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public PooledObject<NettyMQClient> makeObject() throws Exception {
        NettyMQClient client = new NettyMQClient(host, port);
        client.start();
        return new DefaultPooledObject<>(client);
    }

    @Override
    public void destroyObject(PooledObject<NettyMQClient> p) throws Exception {
        p.getObject().close();
    }

    @Override
    public boolean validateObject(PooledObject<NettyMQClient> p) {
        return p.getObject().isActive();
    }

    @Override
    public void activateObject(PooledObject<NettyMQClient> p) throws Exception {
        // No activation required
    }

    @Override
    public void passivateObject(PooledObject<NettyMQClient> p) throws Exception {
        // No passivation required
    }

}
