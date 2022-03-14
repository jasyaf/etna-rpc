package org.etnaframework.rpc.exception;

/**
 * 进行RPC调用时远程服务器执行时抛出的异常
 *
 * @author BlackCat
 * @since 2015-04-11
 */
public class RpcServerInvocationException extends RpcException {

    private static final long serialVersionUID = -6291580032892838585L;

    public RpcServerInvocationException() {
    }

    public RpcServerInvocationException(String msg) {
        super(msg);
    }

    public RpcServerInvocationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
