package org.etnaframework.rpc.codec;

import java.io.Serializable;
import java.util.Map;
import org.etnaframework.core.util.JsonObjectUtils;
import org.slf4j.MDC;

/**
 * RPC底层发起的请求
 *
 * @author BlackCat
 * @since 2015-04-11
 */
public class RpcRequest implements Serializable {

    private static final long serialVersionUID = -1995620584672619944L;

    /** 客户端的消息ID，服务器回包时将会返回相同的ID，以便客户端知道是对应的哪个请求，并执行后续的操作 */
    public long sequence;

    /** 方法签名，包含接口类名、方法名、参数类型等，是可以唯一标识一个方法的字符串 */
    public String signature;

    /** 方法参数列表 */
    public Object[] args;

    /** 日志线程信息，用于在服务端打日志也能追溯到来源 */
    public Map<String, String> mdc;

    public RpcRequest() {
    }

    public RpcRequest(String signature, Object[] args) {
        this.signature = signature;
        this.args = args;
        this.mdc = MDC.getCopyOfContextMap();
    }

    @Override
    public String toString() {
        return "RpcRequest [sequence=" + sequence + ", signature=" + signature + ", args=" + JsonObjectUtils.createJson(args) + "]";
    }
}
