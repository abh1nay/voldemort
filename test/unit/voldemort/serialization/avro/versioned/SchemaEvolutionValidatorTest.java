package voldemort.serialization.avro.versioned;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import voldemort.VoldemortException;
import voldemort.serialization.SerializerDefinition;

public class SchemaEvolutionValidatorTest {

    static String versionZero = "{\"type\": \"record\", \"name\": \"testRecord\", "
                                + "\"fields\" :[{\"name\": \"field1\", \"type\": \"long\"}  ]}";

    static String versionOne = "{\"type\": \"record\", \"name\": \"testRecord\", "
                               + "\"fields\" :[{\"name\": \"field1\", \"type\": \"long\"}, {\"name\": \"field2\", \"type\": [\"null\",\"string\"] }  ]}";

    final String AVRO_GENERIC_VERSIONED_TYPE_NAME = "avro-generic-versioned";

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
