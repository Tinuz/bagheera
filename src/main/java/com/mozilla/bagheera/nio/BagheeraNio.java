/*
 * Copyright 2011 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.bagheera.nio;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import redis.clients.jedis.Jedis;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;


public class BagheeraNio {
    
    public static void main(String[] args) {
        boolean useUDP = (args.length > 0 && args[0] == "-u");
        boolean useTCP = (args.length > 0 && args[0] == "-t");
        int port = Integer.parseInt(System.getProperty("server.port", "8080"));
        
        if (useUDP) {
            // UDP
            NioDatagramChannelFactory channelFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool(), 16);
            ConnectionlessBootstrap cb = new ConnectionlessBootstrap(channelFactory);
            cb.setPipelineFactory(new ServerPipelineFactory());
            cb.bind(new InetSocketAddress(port));  
        } else if (useTCP) { 
            // TCP
            NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
            ServerBootstrap sb = new ServerBootstrap(channelFactory);
            sb.setPipelineFactory(new ServerPipelineFactory());
            
            // Options for a parent channel
            sb.setOption("localAddress", new InetSocketAddress(port));
            sb.setOption("reuseAddress", true);

            // Options for its children
            sb.setOption("child.tcpNoDelay", true);
            sb.setOption("child.receiveBufferSize", 1048576);
            
            sb.bind(new InetSocketAddress(port));
        } else {
            Jedis jedis = new Jedis("localhost");
            
            // HTTP
            NioServerSocketChannelFactory channelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
            ServerBootstrap sb = new ServerBootstrap(channelFactory);
            sb.setPipelineFactory(new HttpServerPipelineFactory());
            sb.bind(new InetSocketAddress(port));
        }
        
//        boolean initHazelcast = Boolean.parseBoolean(System.getProperty("init.hazelcast.onstartup", "true"));
//        if (initHazelcast) {
//            // Initialize Hazelcast now rather than waiting for the first request
//            Hazelcast.getDefaultInstance();
//            Config config = Hazelcast.getConfig();
//            for (Map.Entry<String, MapConfig> entry : config.getMapConfigs().entrySet()) {
//                String mapName = entry.getKey();
//                // If the map contains a wildcard then we need to wait to initialize
//                if (!mapName.contains("*")) {
//                    Hazelcast.getMap(entry.getKey());
//                }
//            }
//        }
    }
    
}
