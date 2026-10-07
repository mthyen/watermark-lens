package io.github.mthyen.wmlens.generator;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/**
 * Checkpoint serializer for a single GeneratorSplit. NumberSequenceSource's split serializer
 * (org.apache.flink.api.connector.source.lib.NumberSequenceSource) is a short, worked example
 * of this pattern using DataOutputSerializer/DataInputDeserializer.
 */
public class GeneratorSplitSerializer implements SimpleVersionedSerializer<GeneratorSplit> {
    private static final int CURRENT_VERSION = 1;
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(GeneratorSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeUTF(split.splitId());
        out.writeLong(split.nextSequence());
        return out.getCopyOfBuffer();
    }

    @Override
    public GeneratorSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) throw new IOException("Unrecognized version: " + version);
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        return new GeneratorSplit(in.readUTF(), in.readLong());
    }
}
