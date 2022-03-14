package org.etnaframework.rpc.codec;

import java.util.List;
import org.jboss.serial.io.JBossObjectOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * 使用jboss-serialization对传输的数据编码
 *
 * @author BlackCat
 * @since 2015-04-18
 */
public class JBossSerializationEncoder extends MessageToMessageEncoder<Object> {

    private static final byte[] LENGTH_PLACEHOLDER = new byte[Integer.SIZE / Byte.SIZE];

    private ChannelInboundHandler handler;

    public JBossSerializationEncoder(ChannelInboundHandler handler) {
        this.handler = handler;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        try {
            ByteBufOutputStream bout = new ByteBufOutputStream(Unpooled.buffer(LENGTH_PLACEHOLDER.length));
            bout.write(LENGTH_PLACEHOLDER);
            JBossObjectOutputStream oout = new JBossObjectOutputStream(bout);
            try {
                oout.writeObject(msg);
                oout.flush();
            } finally {
                oout.close();
            }
            ByteBuf encoded = bout.buffer();
            encoded.setInt(0, encoded.writerIndex() - LENGTH_PLACEHOLDER.length);
            out.add(encoded);
        } catch (Throwable cause) {
            handler.exceptionCaught(ctx, cause);
        }
    }
}
