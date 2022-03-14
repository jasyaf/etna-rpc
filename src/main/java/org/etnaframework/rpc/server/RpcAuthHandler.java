package org.etnaframework.rpc.server;

import org.etnaframework.core.web.AuthHandler;
import org.springframework.stereotype.Service;
import io.netty.channel.ChannelHandlerContext;

/**
 * 远程服务器使用RPC服务的验证条件，类似http的{@link AuthHandler}，只需要实现它然后加{@link Service}注解加入spring扫描路径即可
 *
 * @author BlackCat
 * @since 2015-07-03
 */
public interface RpcAuthHandler {

    /**
     * 鉴别是否有权限使用本机的服务，如果允许使用返回true否则返回false（RPC框架将断开连接）
     */
    public boolean auth(ChannelHandlerContext ctx) throws Exception;
}
