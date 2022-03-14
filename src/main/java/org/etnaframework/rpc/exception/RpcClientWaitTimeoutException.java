package org.etnaframework.rpc.exception;

/**
 * 进行RPC调用时客户端等待服务器返回结果超时
 *
 * @author BlackCat
 * @since 2015-04-11
 */
public class RpcClientWaitTimeoutException extends RpcException {

    private static final long serialVersionUID = -6291580032892838585L;

    public RpcClientWaitTimeoutException() {
    }

    public RpcClientWaitTimeoutException(String msg) {
        super(msg);
    }

    public RpcClientWaitTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
