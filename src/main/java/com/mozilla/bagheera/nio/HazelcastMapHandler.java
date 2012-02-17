/*
 * Copyright 2012 Mozilla Foundation
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

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_ACCEPTABLE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.net.URI;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.mozilla.bagheera.nio.codec.http.InvalidPathException;
import com.mozilla.bagheera.nio.codec.http.PathDecoder;
import com.mozilla.bagheera.nio.codec.json.InvalidJsonException;

public class HazelcastMapHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = Logger.getLogger(HazelcastMapHandler.class);

    // REST path indices
    public static final int ENDPOINT_PATH_IDX = 0;
    public static final int NAMESPACE_PATH_IDX = 1;
    public static final int ID_PATH_IDX = 2;

    // REST endpoints
    private static final String ENDPOINT_SUBMIT = "submit";
    private static final String ENDPOINT_STATS = "stats";
    
    // Specialized REST namespaces
    private static final String NS_METRICS = "metrics";
    
    private MetricsProcessor metricsProcessor;
    
    public HazelcastMapHandler(MetricsProcessor metricsProcessor) {
        this.metricsProcessor = metricsProcessor;
    }
 
    private void handlePost(MessageEvent e, HttpRequest request, String namespace, String id, IMap<String,String> m) {
        HttpResponseStatus status = NOT_FOUND;
        ChannelBuffer content = request.getContent();
        if (content.readable()) {
            if (NS_METRICS.equals(namespace)) {
                status = metricsProcessor.process(m, id, content.toString(CharsetUtil.UTF_8), 
                                                  e.getChannel().getRemoteAddress().toString(), 
                                                  request.getHeader("X-Obsolete-Document"));
            } else {
                m.put(id, content.toString(CharsetUtil.UTF_8));
                status = CREATED;
            }
        }
        writeResponse(status, e, URI.create(id).toString());
    }
    
    private void handleGet(MessageEvent e, HttpRequest request, String namespace, String id, IMap<String,String> m) {
        String v = m.get(id);
        if (v != null) {
            writeResponse(OK, e, m.get(id));
        } else {
            writeResponse(NO_CONTENT, e, null);
        }
    }
    
    private void handleDelete(MessageEvent e, HttpRequest request, String namespace, String id, IMap<String,String> m) {  
        String v = m.remove(id);
        if (v != null) {
            writeResponse(OK, e, null);
        } else {
            writeResponse(NO_CONTENT, e, null);
        }
    }
    
    private void writeResponse(HttpResponseStatus status, MessageEvent e, String entity) {
        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
        response.addHeader(CONTENT_TYPE, "plain/text");
        if (entity != null) {
            ChannelBuffer buf = ChannelBuffers.wrappedBuffer(entity.getBytes(CharsetUtil.UTF_8));
            response.setContent(buf);
            response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
        }

        // Write response
        ChannelFuture future = e.getChannel().write(response);
        future.addListener(ChannelFutureListener.CLOSE);
    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            Object msg = e.getMessage();
            if (msg instanceof HttpRequest) {
                HttpRequest request = (HttpRequest) e.getMessage();
                PathDecoder pd = new PathDecoder(request.getUri());
                String endpoint = pd.getPathElement(ENDPOINT_PATH_IDX);
                if (endpoint != null && ENDPOINT_SUBMIT.equals(endpoint)) {
                    String namespace = pd.getPathElement(NAMESPACE_PATH_IDX);
                    String id = pd.getPathElement(ID_PATH_IDX);
                    if (id == null) {
                        id = UUID.randomUUID().toString();
                    }
                    
                    IMap<String,String> m = Hazelcast.getMap(namespace);
                    if (request.getMethod() == HttpMethod.POST || request.getMethod() == HttpMethod.PUT) {
                        handlePost(e, request, namespace, id, m);
                    } else if (request.getMethod() == HttpMethod.GET) {
                        handleGet(e, request, namespace, id, m);
                    } else if (request.getMethod() == HttpMethod.DELETE) {
                        handleDelete(e, request, namespace, id, m);
                    } else {
                        writeResponse(NOT_FOUND, e, null);
                    }
                } else if (endpoint != null && ENDPOINT_STATS.equals("endpoint")) {
                    // TODO: implement stats 
                    writeResponse(OK, e, null);
                } else {
                    String userAgent = request.getHeader("User-Agent");
                    String remoteIpAddress = e.getChannel().getRemoteAddress().toString();
                    LOG.warn(String.format("Tried to access invalid resource - \"%s\" \"%s\"", remoteIpAddress, userAgent));
                    writeResponse(NOT_ACCEPTABLE, e, null);
                }
            } else {
                writeResponse(INTERNAL_SERVER_ERROR, e, null);
            }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable cause = e.getCause();
        LOG.error(cause.getMessage());
        
        HttpResponse response = null;
        if (cause instanceof InvalidJsonException) {
            response = new DefaultHttpResponse(HTTP_1_1, NOT_ACCEPTABLE);
        } else if (cause instanceof TooLongFrameException) {
            response = new DefaultHttpResponse(HTTP_1_1, REQUEST_ENTITY_TOO_LARGE);
        } else if (cause instanceof InvalidPathException) {
            response = new DefaultHttpResponse(HTTP_1_1, NOT_FOUND);
        } else if (cause instanceof SecurityException) {
            response = new DefaultHttpResponse(HTTP_1_1, FORBIDDEN);
        } else {        
            response = new DefaultHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR);
        }
        
        response.addHeader(CONTENT_TYPE, "plain/text");
        ChannelFuture future = e.getChannel().write(response);
        future.addListener(ChannelFutureListener.CLOSE);
    }
    
}