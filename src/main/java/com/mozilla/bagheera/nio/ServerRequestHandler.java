package com.mozilla.bagheera.nio;

import java.util.Map;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.hazelcast.core.Hazelcast;
import com.mozilla.bagheera.BagheeraProto.DataPacket;
import com.mozilla.bagheera.BagheeraProto.DataPacket.PayloadType;

public class ServerRequestHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        DataPacket dp = (DataPacket)e.getMessage();
        if (dp.hasNamespace() && dp.hasPayload() && dp.hasPayloadType()) {
            if (dp.getPayloadType() == PayloadType.JSON || dp.getPayloadType() == PayloadType.TEXT) {
                Map<String,String> m = Hazelcast.getMap(dp.getNamespace());
                m.put(new String(dp.getStringId()), dp.getPayload().toStringUtf8());
            }
        }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
    
}
