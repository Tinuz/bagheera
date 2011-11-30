package com.mozilla.bagheera.nio;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;

import com.mozilla.bagheera.BagheeraProto.DataPacket;

public class ServerPipelineFactory implements ChannelPipelineFactory {

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline =  Channels.pipeline();
        
        pipeline.addLast("decoder", new ProtobufDecoder(DataPacket.getDefaultInstance()));
        pipeline.addLast("handler", new ServerRequestHandler());
        
        return pipeline;
    }

}
