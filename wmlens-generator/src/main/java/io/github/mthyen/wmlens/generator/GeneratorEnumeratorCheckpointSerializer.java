package io.github.mthyen.wmlens.generator;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Checkpoint serializer for the enumerator's state: the remaining unassigned splits.
 */
public class GeneratorEnumeratorCheckpointSerializer
        implements SimpleVersionedSerializer<Collection<GeneratorSplit>> {
    private static final int CURRENT_VERSION = 1;
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Collection<GeneratorSplit> splits) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer( splits.size() * 32 + 4);
        out.writeInt(splits.size());          // how many are coming
        for (GeneratorSplit split : splits) { // then each one's data, same as the single-split serializer
            out.writeUTF(split.splitId());
            out.writeLong(split.nextSequence());
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public Collection<GeneratorSplit> deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) throw new IOException("Unrecognized version: " + version);
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        int count = in.readInt();                          // read the count back first
        List<GeneratorSplit> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(new GeneratorSplit(in.readUTF(), in.readLong()));  // one split per iteration
        }
        return result;
    }
}
