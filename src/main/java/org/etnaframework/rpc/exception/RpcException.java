package org.etnaframework.rpc.exception;

/**
 * 进行RPC调用时抛出的异常基类
 *
 * @author BlackCat
 * @since 2015-04-11
 */
public abstract class RpcException extends RuntimeException {

    private static final long serialVersionUID = 6471005417710770418L;

    public RpcException() {
    }

    public RpcException(String msg) {
        super(msg);
    }

    public RpcException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
