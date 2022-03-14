package org.etnaframework.rpc.codec;

import org.jboss.serial.io.JBossObjectInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * 使用jboss-serialization对传输的数据解码
 *
 * @author BlackCat
 * @since 2015-04-18
 */
public class JBossSerializationDecoder extends LengthFieldBasedFrameDecoder {

    private ChannelInboundHandler handler;

    public JBossSerializationDecoder(int maxFrameLength, ChannelInboundHandler handler) {
        super(maxFrameLength, 0, 4, 0, 4);
        this.handler = handler;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        try {
            ByteBuf frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }
            ByteBufInputStream is = new ByteBufInputStream(frame);
            JBossObjectInputStream jim = new JBossObjectInputStream(is);
            try {
                Object o = jim.readObject();
                return o;
            } finally {
                jim.close();
            }
        } catch (Throwable cause) {
            handler.exceptionCaught(ctx, cause);
            return null;
        }
    }

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.slice(index, length);
    }
}
