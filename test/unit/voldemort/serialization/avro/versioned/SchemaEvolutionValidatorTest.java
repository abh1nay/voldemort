package voldemort.serialization.avro.versioned;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.junit.Test;

import voldemort.VoldemortException;
import voldemort.serialization.SerializationException;
import voldemort.serialization.SerializerDefinition;

public class SchemaEvolutionValidatorTest {

    static String versionZero = "{\"type\": \"record\", \"name\": \"testRecord\", "
                                + "\"fields\" :[{\"name\": \"field1\", \"type\": \"long\"}  ]}";

    static String versionOne = "{\"type\": \"record\", \"name\": \"testRecord\", "
                               + "\"fields\" :[{\"name\": \"field1\", \"type\": \"long\"}, {\"name\": \"field2\", \"type\": [\"null\",\"string\"] }  ]}";

    static String versionOneWorks = "{\"type\": \"record\", \"name\": \"testRecord\", "
                                    + "\"fields\" :[{\"name\": \"field1\", \"type\": \"long\"}, {\"name\": \"field2\", \"type\": [\"null\",\"string\"], \"default\":null }  ]}";

    final String AVRO_GENERIC_VERSIONED_TYPE_NAME = "avro-generic-versioned";

    @Test
    public void testAvro() {
        Schema s0 = Schema.parse(versionZero);
        Schema s1 = Schema.parse(versionOne);

        Map<Integer, String> versions = new HashMap<Integer, String>();

        versions.put(0, versionZero);
        versions.put(1, versionOne);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = new BinaryEncoder(output);
        GenericDatumWriter<Object> datumWriter = null;
        GenericData.Record record = new GenericData.Record(s0);
        record.put("field1", 1L);

        try {
            datumWriter = new GenericDatumWriter<Object>(s0);
            datumWriter.write(record, encoder);
            encoder.flush();
        } catch(SerializationException sE) {
            throw sE;
        } catch(IOException e) {
            throw new SerializationException(e);
        }
        byte[] schemaZeroBytes = output.toByteArray();

        Decoder decoder = DecoderFactory.defaultFactory()
                                        .createBinaryDecoder(schemaZeroBytes, null);
        GenericDatumReader<Object> reader = null;
        try {
            reader = new GenericDatumReader<Object>(s0, s1);
            // writer's schema
            reader.setSchema(s0);
            // Reader's schema
            reader.setExpected(s1);
            reader.read(null, decoder);
        } catch(IOException e) {
            throw new SerializationException(e);
        }

    }

    /*
     * This tests if a client tries to deserialize an object created using an
     * old schema is successful or not
     */
    @Test
    public void testSchemaEvolutionValidator() throws IOException {

        boolean thrown = false;
        Map<Integer, String> versions = new HashMap<Integer, String>();
        versions.put(0, versionZero);
        versions.put(1, versionOne);

        try {
            SerializerDefinition mySerDef = new SerializerDefinition(AVRO_GENERIC_VERSIONED_TYPE_NAME,
                                                                     versions,
                                                                     true,
                                                                     null);
            SchemaEvolutionValidator.checkSchemaCompatibility(mySerDef);

        } catch(VoldemortException vE) {
            thrown = true;
        }
        assertTrue(thrown);

    }
}
