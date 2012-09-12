package voldemort.store.readonly.mr;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class JobState {

    private int numChunks;
    private Cluster cluster;
    private StoreDefinition storeDef;
    private boolean saveKeys;
    private boolean reducerPerBucket;

    public void configure(JobConf conf) {
        this.cluster = new ClusterMapper().readCluster(new StringReader(conf.get("cluster.xml")));
        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(conf.get("stores.xml")));
        if(storeDefs.size() != 1)
            throw new IllegalStateException("Expected to find only a single store, but found multiple!");
        this.storeDef = storeDefs.get(0);

        this.numChunks = conf.getInt("num.chunks", -1);
        if(this.numChunks < 1)
            throw new VoldemortException("num.chunks not specified in the job conf.");

        this.saveKeys = conf.getBoolean("save.keys", false);
        this.reducerPerBucket = conf.getBoolean("reducer.per.bucket", false);
    }

    @SuppressWarnings("unused")
    public void close() throws IOException {}

    public Cluster getCluster() {
        checkNotNull(cluster);
        return cluster;
    }

    public boolean getSaveKeys() {
        return this.saveKeys;
    }

    public boolean getReducerPerBucket() {
        return this.reducerPerBucket;
    }

    public StoreDefinition getStoreDef() {
        checkNotNull(storeDef);
        return storeDef;
    }

    public String getStoreName() {
        checkNotNull(storeDef);
        return storeDef.getName();
    }

    private final void checkNotNull(Object o) {
        if(o == null)
            throw new VoldemortException("Not configured yet!");
    }

    public int getNumChunks() {
        return this.numChunks;
    }

}
