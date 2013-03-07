package voldemort.serialization.json;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import voldemort.serialization.Serializer;

import com.google.common.collect.ImmutableList;

public class JsonSchemaEvolutionTest {

    static String versionZero = "[{\"action\":\"int8\", " + "\"date\":\"date\","
                                + "\"bcookie\":\"int32\"," + "\"ip\":\"int32\","
                                + "\"asn\":\"int32\"," + "\"user-agent\":\"int32\","
                                + "\"country_code\":\"string\"," + "\"zip\":\"string\"}]";

    static String versionOne = "[{\"action\":\"int8\", " + "\"date\":\"date\","
                               + "\"bcookie\":\"int32\"," + "\"ip\":\"int32\","
                               + "\"asn\":\"int32\"," + "\"user-agent\":\"int32\","
                               + "\"country_code\":\"string\"," + "\"zip\":\"string\","
                               + "\"browserId\":\"int32\"" + "}]";

    /*
     * This tests if a client tries to deserialize an object created using an
     * old schema is successful or not
     */
    @Test
    public void testJsonSchemaEvolution() throws IOException {

        Map<Integer, JsonTypeDefinition> versions = new HashMap<Integer, JsonTypeDefinition>();
        versions.put(0, JsonTypeDefinition.fromJson(versionZero));
        Serializer jsonSer = new JsonTypeSerializer(versions);

        // first test where we just have one of the schema types

        Map<String, Object> m = makeMapData();

        jsonSer.toBytes(ImmutableList.of(m));
        // case 2 we have both the schemas now
        versions.put(1, JsonTypeDefinition.fromJson(versionOne));
        jsonSer = new JsonTypeSerializer(versions);

        jsonSer.toBytes(ImmutableList.of(m));

    }

    private static Map<String, Object> makeMapData() {
        Map<String, Object> m = new HashMap<String, Object>();

        int i = 1;
        String string = Integer.toString(i);

        m.put("action", (byte) i);
        m.put("date", new Date(i));
        m.put("bcookie", i);
        m.put("ip", i);
        m.put("asn", i);
        m.put("user-agent", i);
        m.put("country_code", string);
        m.put("zip", null);

        return m;
    }
}
