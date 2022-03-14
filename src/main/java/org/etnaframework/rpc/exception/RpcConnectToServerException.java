package org.etnaframework.rpc.exception;

/**
 * 进行RPC调用时客户端连接远程服务器失败抛出的异常
 *
 * @author BlackCat
 * @since 2015-04-11
 */
public class RpcConnectToServerException extends RpcException {

    private static final long serialVersionUID = -6291580032892838585L;

    public RpcConnectToServerException() {
    }

    public RpcConnectToServerException(String msg) {
        super(msg);
    }

    public RpcConnectToServerException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
