package com.mozilla.bagheera.nio;

import java.util.concurrent.Executor;

import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import redis.clients.jedis.Jedis;

public class BagheeraNioServerSocketChannelFactory extends NioServerSocketChannelFactory {

    private Jedis jedis;
    
    public BagheeraNioServerSocketChannelFactory(Executor bossExecutor, Executor workerExecutor) {
        super(bossExecutor, workerExecutor);
        jedis = new Jedis("localhost");
        jedis.connect();
    }

    public BagheeraNioServerSocketChannelFactory(Executor bossExecutor, Executor workerExecutor, int workerCount) {
        super(bossExecutor, workerExecutor, workerCount);
        jedis = new Jedis("localhost");
        jedis.connect();
    }
    
    public void releaseExternalResources() {
        super.releaseExternalResources();
        if (jedis != null) {
            jedis.disconnect();
        }
    }
    
}
