package voldemort.store.bdb.dataconversion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.StringSerializer;
import voldemort.store.StoreBinaryFormat;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.sleepycat.je.DatabaseEntry;

public class BdbFixVectorClocksTest {

    @Test
    public void getWinningValueEntryTest() {

        StringSerializer strSer = new StringSerializer();
        Short nodes1[] = { 1, 3, 5 };
        Short nodes2[] = { 1, 2, 3, 4, 5 };
        Short nodes3[] = { 1, 3, 5 };
        Short nodes4[] = { 3, 5, 1 };

        long timestamp = System.currentTimeMillis();

        long version1 = 20130501203L;
        long version2 = 20130501204L;
        long version3 = 20130501206L; // winning version
        long version4 = 20130501205L;

        String key = "key";
        String valueStr1 = "version1";
        String valueStr2 = "version2";
        String valueStr3 = "version3";
        String valueStr4 = "version4";

        List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>();

        List<ClockEntry> clockEntries = new ArrayList<ClockEntry>();
        for(short node: Arrays.asList(nodes1)) {
            clockEntries.add(new ClockEntry(node, version1));
        }
        VectorClock vc1 = new VectorClock(clockEntries, timestamp);

        values.add(Versioned.value((strSer.toBytes(valueStr1)), vc1));

        // add second duplicate
        clockEntries = new ArrayList<ClockEntry>();
        for(short node: Arrays.asList(nodes2)) {
            clockEntries.add(new ClockEntry(node, version2));
        }
        VectorClock vc2 = new VectorClock(clockEntries, timestamp);

        values.add(Versioned.value((strSer.toBytes(valueStr2)), vc2));

        // add 3rd duplicate
        clockEntries = new ArrayList<ClockEntry>();
        for(short node: Arrays.asList(nodes3)) {
            clockEntries.add(new ClockEntry(node, version3));
        }
        VectorClock vc3 = new VectorClock(clockEntries, timestamp);

        values.add(Versioned.value((strSer.toBytes(valueStr3)), vc3));

        // add 4th duplicate
        clockEntries = new ArrayList<ClockEntry>();
        for(short node: Arrays.asList(nodes4)) {
            clockEntries.add(new ClockEntry(node, version4));
        }
        VectorClock vc4 = new VectorClock(clockEntries, timestamp);

        values.add(Versioned.value((strSer.toBytes(valueStr4)), vc4));

        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();
        keyEntry.setData(strSer.toBytes(key));

        byte[] valueVersionedBytes = StoreBinaryFormat.toByteArray(values);

        valueEntry.setData(valueVersionedBytes);

        RoutingStrategy storeRoutingStrategy = Mockito.mock(RoutingStrategy.class);
        Node node1 = Mockito.mock(Node.class);
        Node node2 = Mockito.mock(Node.class);
        Node node3 = Mockito.mock(Node.class);

        Mockito.when(node1.getId()).thenReturn(1);
        Mockito.when(node2.getId()).thenReturn(3);
        Mockito.when(node3.getId()).thenReturn(5);

        List<Node> nodeList = new ArrayList();
        nodeList.add(node1);
        nodeList.add(node2);
        nodeList.add(node3);

        Mockito.when(storeRoutingStrategy.routeRequest(Mockito.any(byte[].class)))
               .thenReturn(nodeList);

        DatabaseEntry winningEntry = BdbFixVectorClocks.getWinningValueEntry(storeRoutingStrategy,
                                                                             keyEntry,
                                                                             valueEntry);
        List<Versioned<byte[]>> returnvalues = StoreBinaryFormat.fromByteArray(winningEntry.getData());

        Assert.assertEquals(returnvalues.size(), 1);

        byte[] winningValue = returnvalues.get(0).getValue();

        String winningStr = strSer.toObject(winningValue);

        Assert.assertEquals(winningStr, valueStr3);
    }
}
