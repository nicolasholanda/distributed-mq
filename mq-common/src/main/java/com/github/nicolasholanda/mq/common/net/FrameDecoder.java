package com.github.nicolasholanda.mq.common.net;

import com.github.nicolasholanda.mq.common.protocol.Protocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import java.util.List;

public class FrameDecoder extends ByteToMessageDecoder {

    private final int maxFrameSize;

    public FrameDecoder() {
        this(Protocol.MAX_REQUEST_SIZE);
    }

    public FrameDecoder(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < Integer.BYTES) {
            return;
        }
        int size = in.getInt(in.readerIndex());
        if (size <= 0 || size > maxFrameSize) {
            throw new CorruptedFrameException("Invalid frame size: " + size);
        }
        if (in.readableBytes() < Integer.BYTES + size) {
            return;
        }
        in.skipBytes(Integer.BYTES);
        out.add(in.readRetainedSlice(size));
    }
}
