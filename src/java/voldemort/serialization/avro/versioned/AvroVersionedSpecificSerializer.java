package voldemort.serialization.avro.versioned;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import voldemort.serialization.SerializationException;
import voldemort.serialization.SerializationUtils;
import voldemort.serialization.Serializer;

public class AvroVersionedSpecificSerializer<T extends SpecificRecord> implements Serializer<T> {

    private final Class<T> clazz;

    private final Integer newestVersion;

    private final SortedMap<Integer, String> typeDefVersions;

    // reader's schema
    private final Schema typeDef;

    /**
     * Constructor accepting a Java class name under the convention
     * java=classname.
     * 
     * @param schemaInfo information on the schema for the serializer.
     */
    @SuppressWarnings("unchecked")
    public AvroVersionedSpecificSerializer(String schemaInfo) {

        this.typeDefVersions = new TreeMap<Integer, String>();
        this.typeDefVersions.put(0, schemaInfo);
        newestVersion = typeDefVersions.lastKey();
        typeDef = Schema.parse(typeDefVersions.get(newestVersion));
        try {
            clazz = (Class<T>) Class.forName(SerializationUtils.getJavaClassFromSchemaInfo(schemaInfo));
            if(!SpecificRecord.class.isAssignableFrom(clazz))
                throw new IllegalArgumentException("Class provided should implement SpecificRecord");
        } catch(ClassNotFoundException e) {
            throw new SerializationException(e);
        }
    }

    public AvroVersionedSpecificSerializer(Map<Integer, String> typeDefVersions) {

        this.typeDefVersions = new TreeMap<Integer, String>(typeDefVersions);
        newestVersion = this.typeDefVersions.lastKey();
        typeDef = Schema.parse(typeDefVersions.get(newestVersion));
        try {
            clazz = (Class<T>) Class.forName(SerializationUtils.getJavaClassFromSchemaInfo(typeDefVersions.get(newestVersion)));
            if(!SpecificRecord.class.isAssignableFrom(clazz))
                throw new IllegalArgumentException("Class provided should implement SpecificRecord");
        } catch(ClassNotFoundException e) {
            throw new SerializationException(e);
        }

    }

    public byte[] toBytes(T object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = new BinaryEncoder(output);
        SpecificDatumWriter<T> datumWriter = null;

        // what if written using an old schema?

        Integer writerVersion = getSchemaVersion(object.getClass().getName());
        output.write(writerVersion.byteValue());
        try {
            datumWriter = new SpecificDatumWriter<T>((Class<T>) object.getClass());

            // SpecificRecord.class.cast(object).getSchema();
            datumWriter.write(object, encoder);
            encoder.flush();
        } catch(IOException e) {
            throw new SerializationException(e);
        } finally {
            SerializationUtils.close(output);
        }
        return output.toByteArray();
    }

    private Integer getSchemaVersion(String clazzStr) throws SerializationException {
        for(Entry<Integer, String> entry: typeDefVersions.entrySet()) {
            String version = entry.getValue();
            if(clazzStr.equals(version))
                return entry.getKey();

        }

        throw new SerializationException("Writer's schema invalid!");
    }

    public T toObject(byte[] bytes) {

        Integer version = Integer.valueOf(bytes[0]);

        if(version > newestVersion)
            throw new SerializationException("Client needs to rebootstrap! \n Writer's schema version greater than Reader");

        Class<T> writerClazz;
        try {
            writerClazz = (Class<T>) Class.forName(SerializationUtils.getJavaClassFromSchemaInfo(typeDefVersions.get(version)));
            if(!SpecificRecord.class.isAssignableFrom(writerClazz))
                throw new IllegalArgumentException("Class provided should implement SpecificRecord");
        } catch(ClassNotFoundException e) {
            throw new SerializationException(e);
        }

        byte[] dataBytes = new byte[bytes.length - 1];
        System.arraycopy(bytes, 1, dataBytes, 0, bytes.length - 1);
        Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(dataBytes, null);
        SpecificDatumReader<T> reader = null;
        try {
            reader = new SpecificDatumReader<T>(writerClazz);

            return reader.read(null, decoder);
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }
}
