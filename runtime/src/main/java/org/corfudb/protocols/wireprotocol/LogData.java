package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.AutoCloseableByteBuf;
import org.corfudb.util.serializer.Serializers;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mwei on 8/15/16.
 */
public class LogData implements ICorfuPayload<LogData>, IMetadata, ILogData {

    public static final LogData EMPTY = new LogData(DataType.EMPTY);
    public static final LogData HOLE = new LogData(DataType.HOLE);

    @Getter
    final DataType type;

    @Getter
    final byte[] data;

    private transient final AtomicReference<Object> payload = new AtomicReference<>();

    public Object getPayload(CorfuRuntime runtime) {
        Object value = payload.get();
        if (value == null) {
            synchronized (this.payload) {
                value = this.payload.get();
                if (value == null) {
                    if (data == null) {
                        this.payload.set(null);
                    }
                    else {
                        try (AutoCloseableByteBuf copyBuf =
                                new AutoCloseableByteBuf(Unpooled.copiedBuffer(data))) {
                            final Object actualValue =
                                    Serializers.CORFU.deserialize(copyBuf, runtime);
                            // TODO: Remove circular dependency on logentry.
                            if (actualValue instanceof LogEntry) {
                                ((LogEntry) actualValue).setEntry(this);
                                ((LogEntry) actualValue).setRuntime(runtime);
                            }
                            value = actualValue == null ? this.payload : actualValue;
                            this.payload.set(value);
                        }
                    }
                }
            }
        }
        return value;
    }

    @Getter
    final EnumMap<LogUnitMetadataType, Object> metadataMap;

    public LogData(ByteBuf buf) {
        type = ICorfuPayload.fromBuffer(buf, DataType.class);
        if (type == DataType.DATA) {
            data = ICorfuPayload.fromBuffer(buf, byte[].class);
            metadataMap =
                    ICorfuPayload.enumMapFromBuffer(buf,
                            IMetadata.LogUnitMetadataType.class, Object.class);
        } else
        {
            data = null;
            metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
        }
    }

    public LogData(DataType type) {
        this.type = type;
        this.data = null;
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }

    public LogData(final DataType type, final ByteBuf buf) {
        this.type = type;
        this.data = byteArrayFromBuf(buf);
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }

    public LogData(ByteBuf buf, EnumMap<LogUnitMetadataType, Object> metadataMap) {
        this.type = DataType.DATA;
        this.data = byteArrayFromBuf(buf);
        this.metadataMap = metadataMap;
    }

    public byte[] byteArrayFromBuf(final ByteBuf buf) {
        ByteBuf readOnlyCopy = buf.asReadOnly();
        readOnlyCopy.resetReaderIndex();
        byte[] outArray = new byte[readOnlyCopy.readableBytes()];
        readOnlyCopy.readBytes(outArray);
        return outArray;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, type);
        if (type == DataType.DATA) {
            ICorfuPayload.serialize(buf, data);
            ICorfuPayload.serialize(buf, metadataMap);
        }
    }
}
