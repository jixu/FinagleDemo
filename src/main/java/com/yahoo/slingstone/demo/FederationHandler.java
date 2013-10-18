package com.yahoo.slingstone.demo;

import org.jboss.netty.channel.*;


public class FederationHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext cxt, MessageEvent e) {
        System.out.println("Got a message: " + e.getMessage());
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext cxt, ExceptionEvent e) {
        e.getCause().printStackTrace();

        Channel ch = e.getChannel();
        ch.close();
    }
}
