package org.etnaframework.rpc.exception;

/**
 * 进行RPC调用时客户端本地捕获到的异常，如编解码错误等，需要报告出来以改进错误
 *
 * @author BlackCat
 * @since 2015-04-18
 */
public class RpcClientCaughtException extends RpcException {

    private static final long serialVersionUID = -6291580032892838585L;

    public RpcClientCaughtException() {
    }

    public RpcClientCaughtException(String msg) {
        super(msg);
    }

    public RpcClientCaughtException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
