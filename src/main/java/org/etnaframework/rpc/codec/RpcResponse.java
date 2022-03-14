package org.etnaframework.rpc.codec;

import java.io.Serializable;
import org.etnaframework.core.util.JsonObjectUtils;
import org.etnaframework.core.util.StringTools;

/**
 * 远程调用服务器对RPC请求的响应内容
 *
 * @author BlackCat
 * @since 2015-04-11
 */
public class RpcResponse implements Serializable {

    private static final long serialVersionUID = -6297461401549820153L;

    /** 客户端的消息ID，服务器回包时将会返回相同的ID，以便客户端知道是对应的哪个请求，并执行后续的操作 */
    public long sequence;

    /** 如果在远程服务器执行时出现异常，这里将返回堆栈的文本信息，如果为null表示执行正常 */
    public String error;

    /** 远程执行的结果 */
    public Object result;

    @Override
    public String toString() {
        return "RpcResponse [sequence=" + sequence + ", error=" + StringTools.escapeWhitespace(error) + ", result=" + JsonObjectUtils.createJson(result) + "]";
    }
}
