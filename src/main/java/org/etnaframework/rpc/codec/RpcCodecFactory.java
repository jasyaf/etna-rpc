package org.etnaframework.rpc.codec;

import org.etnaframework.core.spring.annotation.Config;
import org.springframework.stereotype.Service;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;

/**
 * RPC服务传输的编码解码器
 *
 * @author BlackCat
 * @since 2015-04-18
 */
@Service
public final class RpcCodecFactory {

    /** RPC序列化对象最大的大小，单位字节 */
    @Config(value = "ruiz.rpc.common.maxObjectBytes")
    private static int maxObjectBytes = 1024 * 1024 * 10;

    public static ChannelHandler createEncoder(ChannelInboundHandler handler) {
        return new JBossSerializationEncoder(handler);
    }

    public static ChannelHandler createDecoder(ChannelInboundHandler handler) {
        return new JBossSerializationDecoder(maxObjectBytes, handler);
    }
}
