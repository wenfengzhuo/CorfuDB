package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Created by mwei on 1/8/16.
 */
@ToString(callSuper = true)
@NoArgsConstructor
public class SMREntry extends LogEntry implements ISMRConsumable {

    /**
     * The name of the SMR method. Note that this is limited to the size of a short.
     */
    @Getter
    private String SMRMethod;

    /**
     * The arguments to the SMR method, which could be 0.
     */
    @Getter
    private Object[] SMRArguments;

    /**
     * The serializer used to serialize the SMR arguments.
     */
    @Getter
    private ISerializer serializerType;

    /** An undo record, which can be used to undo this method.
     *
     */
    @Getter
    @Setter
    public transient Object undoRecord;

    /** A boolean - true if this record can be undone.
     *
     */
    @Getter
    @Setter
    public transient boolean undoable = false;

    public SMREntry(String SMRMethod, @NonNull Object[] SMRArguments, ISerializer serializer) {
        super(LogEntryType.SMR);
        this.SMRMethod = SMRMethod;
        this.SMRArguments = SMRArguments;
        this.serializerType = serializer;
    }

    /**
     * This function provides the remaining buffer. Child entries
     * should initialize their contents based on the buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        short methodLength = b.readShort();
        byte[] methodBytes = new byte[methodLength];
        b.readBytes(methodBytes, 0, methodLength);
        SMRMethod = new String(methodBytes);
        serializerType = Serializers.getSerializer(b.readByte());
        byte numArguments = b.readByte();
        Object[] arguments = new Object[numArguments];
        for (byte arg = 0; arg < numArguments; arg++) {
            int len = b.readInt();
            ByteBuf objBuf = b.slice(b.readerIndex(), len);
            arguments[arg] = serializerType.deserialize(objBuf, rt);
            b.skipBytes(len);
        }
        SMRArguments = arguments;
    }

    @Override
    public void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeShort(SMRMethod.length());
        b.writeBytes(SMRMethod.getBytes());
        b.writeByte(serializerType.getType());
        b.writeByte(SMRArguments.length);
        Arrays.stream(SMRArguments)
                .forEach(x -> {
                    int lengthIndex = b.writerIndex();
                    b.writeInt(0);
                    serializerType.serialize(x, b);
                    int length = b.writerIndex() - lengthIndex - 4;
                    b.writerIndex(lengthIndex);
                    b.writeInt(length);
                    b.writerIndex(lengthIndex + length + 4);
                });
    }

    @Override
    public List<SMREntry> getSMRUpdates(UUID id) {
        // TODO: we should check that the id matches the id of this entry,
        // but replex erases this information.
        return Collections.singletonList(this);
    }
}
