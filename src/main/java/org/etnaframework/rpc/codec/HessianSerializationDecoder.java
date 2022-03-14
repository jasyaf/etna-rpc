package org.etnaframework.rpc.codec;

import java.io.ByteArrayInputStream;
import java.util.List;
import org.etnaframework.core.logging.Log;
import org.etnaframework.core.util.ZipTools;
import org.slf4j.Logger;
import com.caucho.hessian.io.HessianSerializerInput;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * 使用hessian对传输的数据解码
 *
 * @author BlackCat
 * @since 2016-08-09
 */
public class HessianSerializationDecoder extends ByteToMessageDecoder {

    protected final Logger log = Log.getLogger(getClass());

    private int maxDataLength;

    private ChannelInboundHandler handler;

    public HessianSerializationDecoder(int maxDataLength, ChannelInboundHandler handler) {
        this.maxDataLength = maxDataLength;
        this.handler = handler;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            // 4个字节的总长度信息是必须要有的，如果可读字节数小于这个数就得打回等多读一些
            if (in.readableBytes() < 4) {
                return;
            }
            in.markReaderIndex();
            int length = in.readInt();
            if (length < 1 || length > maxDataLength) { // 包体长度不合范围的直接丢包关连接处理
                log.error("INVALID RPC DATA LENGTH " + length);
                ctx.close();
                return;
            }
            // 可读字节少于包体长度，复位游标等下一次读进来更多的数据
            if (in.readableBytes() < length) {
                in.resetReaderIndex();
                return;
            }
            byte[] dataZip = new byte[length];
            in.readBytes(dataZip);
            ByteArrayInputStream is = new ByteArrayInputStream(ZipTools.ungzip(dataZip));
            HessianSerializerInput him = new HessianSerializerInput(is);
            try {
                Object o = him.readObject();
                if (null != o) {
                    out.add(o);
                }
            } finally {
                him.close();
            }
        } catch (Throwable cause) {
            handler.exceptionCaught(ctx, cause);
        }
    }
}
