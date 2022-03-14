package org.etnaframework.rpc.server;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.etnaframework.core.logging.Log;
import org.etnaframework.core.util.StringTools;
import org.etnaframework.core.util.ThreadUtils;
import org.etnaframework.core.web.DispatchFilter;
import org.etnaframework.rpc.codec.RpcRequest;
import org.etnaframework.rpc.codec.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * RPC服务在服务器端的业务逻辑处理，由于没有状态信息，加了{@link Sharable}标注可以多{@link Channel}共享
 *
 * @author BlackCat
 * @since 2015-04-18
 */
@Service
@Sharable
public class RpcServerHandler extends SimpleChannelInboundHandler<RpcRequest> {

    protected final Logger log = Log.getLogger();

    @Autowired
    private RpcMappers rpcMappers;

    @Autowired(required = false)
    private RpcAuthHandler rpcAuthHandler;

    /**
     * 异常处理，由于是全双工连接只要出现不能处理的异常，就必须把连接断开，否则接下来的数据可能全都乱了
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            log.error("{}", ctx.channel(), cause);
            if (cause instanceof IOException) { // 网络异常，也有可能是soLinger参数引起，此种情况不管
                return;
            }
            // 如果在日志配置了遇到异常发邮件就会自动发出
            String title = cause.getClass().getSimpleName() + ":" + RpcRequest.class.getSimpleName() + "From[" + ctx.channel().remoteAddress() + "]";
            DispatchFilter.sendMail(title, cause);
        } finally {
            ctx.close();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 如果需要对连接鉴权，就先鉴权，否则直接关闭连接不给提供服务
        if (null != rpcAuthHandler && !rpcAuthHandler.auth(ctx)) {
            log.error("refuse RPC connect from {}", ctx.channel().remoteAddress());
            ctx.close();
        } else {
            log.info("accept RPC connect from {}", ctx.channel().remoteAddress());
        }
    }

    /**
     * 收到RPC请求，放入业务线程池处理
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final RpcRequest req) throws Exception {
        log.debug("[{}] -> {}", ctx.channel().remoteAddress(), req);
        ThreadUtils.getDefault().execute(new Runnable() {

            long startTime = System.currentTimeMillis(); // starttime

            @Override
            public void run() {
                final RpcResponse resp = new RpcResponse();
                resp.sequence = req.sequence;
                RpcMeta rm = rpcMappers.getRpcMeta(req.signature);
                Throwable t = null;
                try {
                    if (null != req.mdc) {
                        MDC.setContextMap(req.mdc); // 客户端日志线程信息，用于在服务端打日志也能追溯到来源
                    }
                    if (null == rm) {
                        resp.error = "No Such Method Implement On Server: " + req.signature + "\n" + Thread.currentThread().getName();
                    } else {
                        resp.result = rm.invoke(req.args);
                    }
                } catch (Throwable ex) {
                    if (ex instanceof InvocationTargetException) {
                        t = ((InvocationTargetException) ex).getTargetException();
                    } else {
                        t = ex;
                    }
                    resp.error = StringTools.printThrowable(t) + Thread.currentThread().getName();
                } finally {
                    // 出现异常时需要报告出来，只用判断error是否为空即可
                    if (null != resp.error) {
                        String title = (null == t ? NoSuchMethodException.class.getSimpleName() : t.getClass().getSimpleName()) + ":" + RpcRequest.class.getSimpleName() + "From[" + ctx.channel().remoteAddress() + "]";
                        DispatchFilter.sendMail(title, req.toString(), t);
                    }
                    if (ctx.channel().isActive()) {
                        ctx.channel().writeAndFlush(resp).addListener(new GenericFutureListener<Future<? super Void>>() {

                            @Override
                            public void operationComplete(Future<? super Void> future) throws Exception {
                                log.debug("[{}] <- {}", ctx.channel().remoteAddress(), resp);
                            }
                        });
                    } else {
                        log.error("[{}] <:(Write Failed Channel Disconnected) {}", ctx.channel().remoteAddress(), resp);
                    }
                    if (null != rm) {
                        rm.getStat().record(System.currentTimeMillis(), startTime, rm);
                    }
                    if (null != req.mdc) {
                        MDC.clear(); // 清除当前线程中记录的TAG
                    }
                }
            }
        });
    }
}
