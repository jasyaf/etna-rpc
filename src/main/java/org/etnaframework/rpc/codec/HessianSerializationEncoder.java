package org.etnaframework.rpc.codec;

import java.io.ByteArrayOutputStream;
import java.util.List;
import org.etnaframework.core.util.ZipTools;
import com.caucho.hessian.io.HessianSerializerOutput;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.TooLongFrameException;

/**
 * 使用hessian对传输的数据编码
 *
 * @author BlackCat
 * @since 2016-08-09
 */
public class HessianSerializationEncoder extends MessageToMessageEncoder<Object> {

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4]; // 一个int的长度为4字节

    private int maxDataLength;

    private ChannelInboundHandler handler;

    public HessianSerializationEncoder(int maxDataLength, ChannelInboundHandler handler) {
        this.maxDataLength = maxDataLength;
        this.handler = handler;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            HessianSerializerOutput oout = new HessianSerializerOutput(bout);
            try {
                oout.writeObject(msg);
                oout.flush();
            } finally {
                oout.close();
            }
            byte[] data = ZipTools.gzip(bout.toByteArray());
            if (data.length > maxDataLength) {
                throw new TooLongFrameException("传入的包体长度" + data.length + "过大，上限是" + maxDataLength + "字节，请不要通过RPC传送过大的对象");
            }
            ByteBuf encoded = Unpooled.wrappedBuffer(LENGTH_PLACEHOLDER, data);
            // 写入包体长度信息
            encoded.setInt(0, data.length);
            out.add(encoded);
        } catch (Throwable cause) {
            handler.exceptionCaught(ctx, cause);
        }
    }
}
