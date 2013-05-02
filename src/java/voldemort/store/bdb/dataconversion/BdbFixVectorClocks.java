package voldemort.store.bdb.dataconversion;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * 
 * BdbFixVectorClocks scrapes an input store on a single node and for each key
 * it checks if there are duplicates/concurrent versions for the key It will
 * then pick the corresponding value with the highest clock entry in the vector
 * clock It then writes it to a destination path
 * 
 */

public class BdbFixVectorClocks extends AbstractBdbConversion {

    private final AdminClient adminClient;
    private final RoutingStrategyFactory factory;
    private final RoutingStrategy storeRoutingStrategy;

    static Logger logger = Logger.getLogger(BdbFixVectorClocks.class);

    BdbFixVectorClocks(String storeName,
                       String storesXmlPath,
                       String clusterXmlPath,
                       String sourceEnvPath,
                       String destEnvPath,
                       int logFileSize,
                       int nodeMax) throws Exception {
        super(storeName, clusterXmlPath, sourceEnvPath, destEnvPath, logFileSize, nodeMax);

        StoreDefinitionsMapper storeDefMapper;
        StoreDefinition storeDef = null;

        factory = new RoutingStrategyFactory();

        storeDefMapper = new StoreDefinitionsMapper();

        List<StoreDefinition> storeDefs = storeDefMapper.readStoreList(new File(storesXmlPath));

        for(StoreDefinition storeDefInstance: storeDefs) {
            if(storeDefInstance.getName().equals(storeName)) {
                storeDef = storeDefInstance;
                break;
            }
        }
        if(storeDef == null) {

            System.err.println("Store name not found");
            System.exit(0);
        }

        adminClient = new AdminClient(cluster, new AdminClientConfig(), new ClientConfig());
        storeRoutingStrategy = factory.updateRoutingStrategy(storeDef,
                                                             adminClient.getAdminClientCluster());
    }

    @Override
    public void transfer() throws Exception {
        cursor = srcDB.openCursor(null, null);
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry valueEntry = new DatabaseEntry();

        List<Versioned<byte[]>> vals;
        long startTime = System.currentTimeMillis();
        int scanCount = 0;
        int keyCount = 0;
        while(cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
            keyCount++;

            vals = StoreBinaryFormat.fromByteArray(valueEntry.getData());
            scanCount += vals.size();

            DatabaseEntry winningValueEntry = getWinningValueEntry(storeRoutingStrategy,
                                                                   keyEntry,
                                                                   valueEntry);

            OperationStatus putStatus = dstDB.put(null, keyEntry, // write
                                                                  // the
                                                                  // original
                                                                  // key
                                                                  // without
                                                                  // striping
                                                  winningValueEntry); // write
                                                                      // the
                                                                      // winning
                                                                      // value

            if(OperationStatus.SUCCESS != putStatus) {
                String errorStr = "Put failed with " + putStatus + " for key"
                                  + BdbConvertData.writeAsciiString(keyEntry.getData());
                logger.error(errorStr);
                throw new Exception(errorStr);
            }

            if(scanCount % 1000000 == 0)
                logger.info("Repaired " + scanCount + " entries in "
                            + (System.currentTimeMillis() - startTime) / 1000 + " secs");
        }
        logger.info("Repaired " + scanCount + " entries and " + keyCount + " keys in "
                    + (System.currentTimeMillis() - startTime) / 1000 + " secs");
    }

    /**
     * 
     * @param storeRoutingStrategy The routing strategy object for the store
     * @param keyEntry The Databaseentry containg key bytes
     * @param valueEntry The DatabaseEnntry containing versioned value bytes
     * @return A DatabaseEntry object with just one versioned value with highest
     *         clockentry
     */
    public static DatabaseEntry getWinningValueEntry(RoutingStrategy storeRoutingStrategy,
                                                     DatabaseEntry keyEntry,
                                                     DatabaseEntry valueEntry) {

        DatabaseEntry winningValueEntry = new DatabaseEntry();

        // pull out the real key
        byte[] stripedKey = StoreBinaryFormat.extractKey(keyEntry.getData());

        byte[] winningValue = null;
        long winningVersion = 0L;
        long winningTimeStamp = 0L;
        // pick the one value with the biggest clockentry
        // for every versioned value associated with the key find the value
        // with greatest clockentry
        for(Versioned<byte[]> val: StoreBinaryFormat.fromByteArray(valueEntry.getData())) {
            VectorClock version = (VectorClock) val.getVersion();
            List<ClockEntry> clockEntries = version.getEntries();
            for(ClockEntry clockEntry: clockEntries) {
                if(clockEntry.getVersion() > winningVersion) {
                    winningVersion = clockEntry.getVersion();
                    winningValue = val.getValue();
                    winningTimeStamp = version.getTimestamp();

                }
                // if versions are equal use the latest timestamp
                if(winningVersion == clockEntry.getVersion()
                   && winningTimeStamp < version.getTimestamp()) {
                    winningVersion = clockEntry.getVersion();
                    winningValue = val.getValue();
                    winningTimeStamp = version.getTimestamp();

                }
            }

        }

        List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>();
        values.add(Versioned.value((winningValue),
                                   generateVectorClock(storeRoutingStrategy,
                                                       stripedKey,
                                                       winningVersion,
                                                       winningTimeStamp)));
        byte[] winningValueVersionedBytes = StoreBinaryFormat.toByteArray(values);

        winningValueEntry.setData(winningValueVersionedBytes);

        return winningValueEntry;

    }

    public static Version generateVectorClock(RoutingStrategy storeRoutingStrategy,
                                              byte[] keyBytes,
                                              Long version,
                                              Long timestamp) {
        List<ClockEntry> clockEntries = new ArrayList<ClockEntry>();

        List<Node> nodeList = storeRoutingStrategy.routeRequest(keyBytes);

        // sort the nodes
        Collections.sort(nodeList);

        for(Node node: nodeList) {
            clockEntries.add(new ClockEntry((short) node.getId(), version));
        }
        return new VectorClock(clockEntries, timestamp);
    }

    @Override
    public void close() {
        super.close();
    }

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("cluster-xml", "[REQUIRED] path to cluster.xml file for the server")
              .withRequiredArg()
              .describedAs("cluster-xml")
              .ofType(String.class);
        parser.accepts("stores-xml", "[REQUIRED] path to stores.xml file for the server")
              .withRequiredArg()
              .describedAs("stores-xml")
              .ofType(String.class);
        parser.accepts("src", "[REQUIRED] Source environment to be converted")
              .withRequiredArg()
              .describedAs("source-env")
              .ofType(String.class);
        parser.accepts("dest", "[REQUIRED] Destination environment to place converted data into")
              .withRequiredArg()
              .describedAs("destination-env")
              .ofType(String.class);
        parser.accepts("store", "[REQUIRED] Store/BDB database to convert")
              .withRequiredArg()
              .describedAs("store")
              .ofType(String.class);
        parser.accepts("je-log-size", "[Optional] Size of the converted JE log files")
              .withRequiredArg()
              .describedAs("je-log-size")
              .ofType(Integer.class);
        parser.accepts("btree-nodemax", "[Optional] Fanout of converted Btree nodes")
              .withRequiredArg()
              .describedAs("btree-nodemax")
              .ofType(Integer.class);

        OptionSet options = parser.parse(args);

        if(!options.has("cluster-xml") || !options.has("src") || !options.has("dest")
           || !options.has("store") || !options.has("stores-xml")) {
            parser.printHelpOn(System.err);
            System.exit(0);
        }

        String clusterXmlPath = CmdUtils.valueOf(options, "cluster-xml", null);
        String storesXmlPath = CmdUtils.valueOf(options, "stores-xml", null);
        String sourceEnvPath = CmdUtils.valueOf(options, "src", null);
        String destEnvPath = CmdUtils.valueOf(options, "dest", null);
        String storeName = CmdUtils.valueOf(options, "store", null);

        Integer logFileSize = CmdUtils.valueOf(options, "je-log-size", 60);
        Integer nodeMax = CmdUtils.valueOf(options, "btree-nodemax", 512);

        BdbFixVectorClocks clockFixer = new BdbFixVectorClocks(storeName,
                                                               storesXmlPath,
                                                               clusterXmlPath,
                                                               sourceEnvPath,
                                                               destEnvPath,
                                                               logFileSize,
                                                               nodeMax);

        try {
            clockFixer.transfer();
        } catch(Exception e) {
            logger.error("Error deduplicating data", e);
        } finally {
            if(clockFixer != null)
                clockFixer.close();
        }

    }

    @Override
    public boolean areDuplicatesNeededForSrc() {
        return false;
    }

    @Override
    public boolean areDuplicatesNeededForDest() {
        return false;
    }
}
