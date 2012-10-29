package voldemort.client.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketDestination;
import voldemort.utils.ByteArray;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

public class StreamingClient {

    private String storeToStream = null;
    private Callable checkpointCallback = null;
    private Callable recoveryCallback = null;
    private boolean allowMerge = false;

    private HashMap<Integer, SocketDestination> nodeIdToSocketRequest;
    private HashMap<Integer, DataOutputStream> nodeIdToOutputStreamRequest;
    private HashMap<Integer, DataInputStream> nodeIdToInputStreamRequest;
    private HashMap<Integer, SocketAndStreams> nodeIdToSocketAndStreams;

    private HashMap<Integer, Boolean> nodeIdInitialized;
    private SocketPool streamingSocketPool;

    List<StoreDefinition> remoteStoreDefs;
    protected RoutingStrategy routingStrategy;

    boolean newBatch;
    boolean cleanedUp = false;

    boolean isMultiSession;
    ExecutorService streamingresults;

    private static final int BATCH_SIZE = 1000;
    private static final int THROTTLE_QPS = 3000;
    private int entriesProcessed;

    private static boolean MARKED_BAD = false;

    protected EventThrottler throttler;

    AdminClient adminClient;
    AdminClientConfig adminClientConfig;

    String bootstrapURL;

    public StreamingClient(String bootstrapURL) {
        this.bootstrapURL = bootstrapURL;

    }

    /**
     ** store - the name of the store to be streamed to
     * 
     * checkpointCallback - the callback that allows for the user to record the
     * progress, up to the last event delivered. This callable would be invoked
     * every so often internally.
     * 
     * recoveryCallback - the callback that allows the user to rewind the
     * upstream to the position recorded by the last complete call on
     * checkpointCallback whenever an exception occurs during the streaming
     * session.
     * 
     * allowMerge - whether to allow for the streaming event to be merged with
     * online writes. If not, all online writes since the completion of the last
     * streaming session will be lost at the end of the current streaming
     * session.
     **/
    @SuppressWarnings({ "rawtypes", "unused", "unchecked" })
    public void initStreamingSession(String store,
                                     Callable checkpointCallback,
                                     Callable recoveryCallback,
                                     boolean allowMerge) {

        // TODO
        // internally call sessions with a single store

        adminClientConfig = new AdminClientConfig();
        adminClient = new AdminClient(bootstrapURL, adminClientConfig);
        storeToStream = store;
        this.checkpointCallback = checkpointCallback;
        this.recoveryCallback = recoveryCallback;
        this.allowMerge = allowMerge;
        streamingresults = Executors.newFixedThreadPool(3);
        entriesProcessed = 0;
        newBatch = true;
        cleanedUp = false;
        isMultiSession = false;
        this.throttler = new EventThrottler(THROTTLE_QPS);
        if(allowMerge == false) {

            // new admin call to open BDB in a deferred writes - bulk load mode

            VAdminProto.BDBModeChangeRequest.Builder updateRequest = VAdminProto.BDBModeChangeRequest.newBuilder()
                                                                                                     .setStoreName(storeToStream)
                                                                                                     .setDeferred(true);

            // TODO add check if store already exists throw warning?

        }

        TimeUnit unit = TimeUnit.SECONDS;

        // socket pool with 2 sockets per node
        streamingSocketPool = new SocketPool(adminClient.getAdminClientCluster().getNumberOfNodes() * 2,
                                             (int) unit.toMillis(adminClientConfig.getAdminConnectionTimeoutSec()),
                                             (int) unit.toMillis(adminClientConfig.getAdminSocketTimeoutSec()),
                                             adminClientConfig.getAdminSocketBufferSize(),
                                             adminClientConfig.getAdminSocketKeepAlive());

        nodeIdToSocketRequest = new HashMap();
        nodeIdToOutputStreamRequest = new HashMap();
        nodeIdToInputStreamRequest = new HashMap();
        nodeIdInitialized = new HashMap();
        nodeIdToSocketAndStreams = new HashMap();

        for(Node node: adminClient.getAdminClientCluster().getNodes()) {

            SocketDestination destination = new SocketDestination(node.getHost(),
                                                                  node.getAdminPort(),
                                                                  RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            SocketAndStreams sands = streamingSocketPool.checkout(destination);
            DataOutputStream outputStream = sands.getOutputStream();
            DataInputStream inputStream = sands.getInputStream();

            nodeIdToSocketAndStreams.put(node.getId(), sands);
            nodeIdToSocketRequest.put(node.getId(), destination);
            nodeIdToOutputStreamRequest.put(node.getId(), outputStream);
            nodeIdToInputStreamRequest.put(node.getId(), inputStream);
            nodeIdInitialized.put(node.getId(), false);
            remoteStoreDefs = adminClient.getRemoteStoreDefList(node.getId()).getValue();

        }

        boolean foundStore = false;
        for(StoreDefinition remoteStoreDef: remoteStoreDefs) {
            if(remoteStoreDef.getName().equals(store)) {
                RoutingStrategyFactory factory = new RoutingStrategyFactory();
                this.routingStrategy = factory.updateRoutingStrategy(remoteStoreDef,
                                                                     adminClient.getAdminClientCluster());
                foundStore = true;
                break;
            }
        }

        if(!foundStore) {
            throw new VoldemortException("Store Name not found on the cluster");

        }

    }

    /**
     ** key - The key
     * 
     * value - The value
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void streamingPut(ByteArray key, Versioned<byte[]> value) {

        if(MARKED_BAD)
            throw new VoldemortException("Cannot stream more entries since Recovery Callback Failed!");

        List<Node> nodeList = routingStrategy.routeRequest(key.get());

        // sent the k/v pair to the nodes
        for(Node node: nodeList) {

            VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                  .setKey(ProtoUtils.encodeBytes(key))
                                                                                  .setVersioned(ProtoUtils.encodeVersioned(value))
                                                                                  .build();

            VAdminProto.UpdatePartitionEntriesRequest.Builder updateRequest = VAdminProto.UpdatePartitionEntriesRequest.newBuilder()
                                                                                                                       .setStore(storeToStream)
                                                                                                                       .setPartitionEntry(partitionEntry);

            DataOutputStream outputStream = nodeIdToOutputStreamRequest.get(node.getId());
            try {
                if(nodeIdInitialized.get(node.getId())) {
                    ProtoUtils.writeMessage(outputStream, updateRequest.build());
                } else {
                    ProtoUtils.writeMessage(outputStream,
                                            VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                             .setType(VAdminProto.AdminRequestType.UPDATE_PARTITION_ENTRIES)
                                                                             .setUpdatePartitionEntries(updateRequest)
                                                                             .build());
                    outputStream.flush();
                    nodeIdInitialized.put(node.getId(), true);

                }

                entriesProcessed++;

            } catch(IOException e) {
                Future future = streamingresults.submit(recoveryCallback);
                try {
                    future.get();

                } catch(InterruptedException e1) {
                    MARKED_BAD = true;
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                } catch(ExecutionException e1) {
                    MARKED_BAD = true;
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                }

                e.printStackTrace();
            }

        }

        if(entriesProcessed == BATCH_SIZE) {
            entriesProcessed = 0;
            newBatch = true;

            for(Node node: adminClient.getAdminClientCluster().getNodes()) {

                if(!nodeIdInitialized.get(node.getId()))
                    continue;
                nodeIdInitialized.put(node.getId(), false);
                DataOutputStream outputStream = nodeIdToOutputStreamRequest.get(node.getId());

                try {
                    ProtoUtils.writeEndOfStream(outputStream);
                    outputStream.flush();
                    DataInputStream inputStream = nodeIdToInputStreamRequest.get(node.getId());
                    VAdminProto.UpdatePartitionEntriesResponse.Builder updateResponse = ProtoUtils.readToBuilder(inputStream,
                                                                                                                 VAdminProto.UpdatePartitionEntriesResponse.newBuilder());
                    if(updateResponse.hasError()) {
                        Future future = streamingresults.submit(recoveryCallback);
                        try {
                            future.get();

                        } catch(InterruptedException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        } catch(ExecutionException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                    }
                    Future future = streamingresults.submit(checkpointCallback);
                    try {
                        future.get();

                    } catch(InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    } catch(ExecutionException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }

                } catch(IOException e) {
                    Future future = streamingresults.submit(recoveryCallback);
                    try {
                        future.get();

                    } catch(InterruptedException e1) {
                        MARKED_BAD = true;
                        e1.printStackTrace();
                        throw new VoldemortException("Recovery Callback failed");
                    } catch(ExecutionException e1) {
                        MARKED_BAD = true;
                        e1.printStackTrace();
                        throw new VoldemortException("Recovery Callback failed");
                    }

                    e.printStackTrace();
                }
            }
        }

        throttler.maybeThrottle(1);

    }

    /**
     ** resetCheckpointCallback - the callback that allows for the user to clean
     * up the checkpoint at the end of the streaming session so a new session
     * could, if necessary, start from 0 position.
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void closeStreamingSession(Callable resetCheckpointCallback) {

        closeStreamingSession();

        Future future = streamingresults.submit(resetCheckpointCallback);
        try {
            future.get();

        } catch(InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch(ExecutionException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    /**
     * Close the streaming session Flush all n/w buffers and call the commit
     * callback
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void closeStreamingSession() {

        for(Node node: adminClient.getAdminClientCluster().getNodes()) {

            if(!nodeIdInitialized.get(node.getId()))
                continue;
            DataOutputStream outputStream = nodeIdToOutputStreamRequest.get(node.getId());

            try {
                ProtoUtils.writeEndOfStream(outputStream);
                outputStream.flush();
                DataInputStream inputStream = nodeIdToInputStreamRequest.get(node.getId());
                VAdminProto.UpdatePartitionEntriesResponse.Builder updateResponse = ProtoUtils.readToBuilder(inputStream,
                                                                                                             VAdminProto.UpdatePartitionEntriesResponse.newBuilder());
                if(updateResponse.hasError()) {
                    Future future = streamingresults.submit(recoveryCallback);
                    try {
                        future.get();

                    } catch(InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    } catch(ExecutionException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }
                Future future = streamingresults.submit(checkpointCallback);
                try {
                    future.get();

                } catch(InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                } catch(ExecutionException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }

            } catch(IOException e) {
                Future future = streamingresults.submit(recoveryCallback);
                try {
                    future.get();

                } catch(InterruptedException e1) {
                    MARKED_BAD = true;
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                } catch(ExecutionException e1) {
                    MARKED_BAD = true;
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                }

                e.printStackTrace();
            }
        }

        cleanupSession();

    }

    /*
     * Cleanup a single store session
     */
    private void cleanupSession() {
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {

            SocketAndStreams sands = nodeIdToSocketAndStreams.get(node.getId());
            close(sands.getSocket());
            SocketDestination destination = nodeIdToSocketRequest.get(node.getId());
            streamingSocketPool.checkin(destination, sands);

        }

        cleanedUp = true;
    }

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            // logger.warn("Failed to close socket");
        }
    }

    @Override
    protected void finalize() {
        if(!cleanedUp && !isMultiSession) {
            cleanupSession();
        }

        if(!cleanedUp && isMultiSession) {
            cleanupSessions();
        }

    }

    private HashMap<String, RoutingStrategy> storeToRoutingStrategy;
    private HashMap<Pair<String, Integer>, Boolean> nodeIdStoreInitialized;

    private HashMap<Pair<String, Integer>, SocketDestination> nodeIdStoreToSocketRequest;
    private HashMap<Pair<String, Integer>, DataOutputStream> nodeIdStoreToOutputStreamRequest;
    private HashMap<Pair<String, Integer>, DataInputStream> nodeIdStoreToInputStreamRequest;

    private HashMap<Pair<String, Integer>, SocketAndStreams> nodeIdStoreToSocketAndStreams;

    private List<String> storeNames;

    private final static int MAX_STORES_PER_SESSION = 100;

    /**
     ** stores - the list of name of the stores to be streamed to
     * 
     * checkpointCallback - the callback that allows for the user to record the
     * progress, up to the last event delivered. This callable would be invoked
     * every so often internally.
     * 
     * recoveryCallback - the callback that allows the user to rewind the
     * upstream to the position recorded by the last complete call on
     * checkpointCallback whenever an exception occurs during the streaming
     * session.
     * 
     * allowMerge - whether to allow for the streaming event to be merged with
     * online writes. If not, all online writes since the completion of the last
     * streaming session will be lost at the end of the current streaming
     * session.
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void initStreamingSessions(List<String> stores,
                                      Callable checkpointCallback,
                                      Callable recoveryCallback,
                                      boolean allowMerge) {

        adminClientConfig = new AdminClientConfig();
        adminClient = new AdminClient(bootstrapURL, adminClientConfig);
        this.checkpointCallback = checkpointCallback;
        this.recoveryCallback = recoveryCallback;
        this.allowMerge = allowMerge;
        streamingresults = Executors.newFixedThreadPool(3);
        entriesProcessed = 0;
        newBatch = true;
        isMultiSession = true;
        storeNames = stores;
        this.throttler = new EventThrottler(THROTTLE_QPS);

        TimeUnit unit = TimeUnit.SECONDS;

        // socket pool
        streamingSocketPool = new SocketPool(adminClient.getAdminClientCluster().getNumberOfNodes()
                                                     * MAX_STORES_PER_SESSION,
                                             (int) unit.toMillis(adminClientConfig.getAdminConnectionTimeoutSec()),
                                             (int) unit.toMillis(adminClientConfig.getAdminSocketTimeoutSec()),
                                             adminClientConfig.getAdminSocketBufferSize(),
                                             adminClientConfig.getAdminSocketKeepAlive());

        nodeIdStoreToSocketRequest = new HashMap();
        nodeIdStoreToOutputStreamRequest = new HashMap();
        nodeIdStoreToInputStreamRequest = new HashMap();
        nodeIdStoreInitialized = new HashMap();
        storeToRoutingStrategy = new HashMap();
        nodeIdStoreToSocketAndStreams = new HashMap();
        for(String store: stores) {

            for(Node node: adminClient.getAdminClientCluster().getNodes()) {

                SocketDestination destination = new SocketDestination(node.getHost(),
                                                                      node.getAdminPort(),
                                                                      RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
                SocketAndStreams sands = streamingSocketPool.checkout(destination);
                DataOutputStream outputStream = sands.getOutputStream();
                DataInputStream inputStream = sands.getInputStream();

                nodeIdStoreToSocketRequest.put(new Pair(store, node.getId()), destination);
                nodeIdStoreToOutputStreamRequest.put(new Pair(store, node.getId()), outputStream);
                nodeIdStoreToInputStreamRequest.put(new Pair(store, node.getId()), inputStream);
                nodeIdStoreToSocketAndStreams.put(new Pair(store, node.getId()), sands);
                nodeIdStoreInitialized.put(new Pair(store, node.getId()), false);

                remoteStoreDefs = adminClient.getRemoteStoreDefList(node.getId()).getValue();

            }
        }

        boolean foundStore = false;
        for(String store: stores) {
            for(StoreDefinition remoteStoreDef: remoteStoreDefs) {
                if(remoteStoreDef.getName().equals(store)) {
                    RoutingStrategyFactory factory = new RoutingStrategyFactory();
                    RoutingStrategy storeRoutingStrategy = factory.updateRoutingStrategy(remoteStoreDef,
                                                                                         adminClient.getAdminClientCluster());

                    storeToRoutingStrategy.put(store, storeRoutingStrategy);
                    foundStore = true;
                    break;
                }
            }
            if(!foundStore) {
                throw new VoldemortException("Store Name not found on the cluster");

            }
        }

    }

    /*
     * Add another store destination to an existing streaming session
     */
    private void addStoreToSession(String store) {

        storeNames.add(store);

        for(Node node: adminClient.getAdminClientCluster().getNodes()) {

            SocketDestination destination = new SocketDestination(node.getHost(),
                                                                  node.getAdminPort(),
                                                                  RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            SocketAndStreams sands = streamingSocketPool.checkout(destination);
            DataOutputStream outputStream = sands.getOutputStream();
            DataInputStream inputStream = sands.getInputStream();

            nodeIdStoreToSocketRequest.put(new Pair(store, node.getId()), destination);
            nodeIdStoreToOutputStreamRequest.put(new Pair(store, node.getId()), outputStream);
            nodeIdStoreToInputStreamRequest.put(new Pair(store, node.getId()), inputStream);
            nodeIdStoreToSocketAndStreams.put(new Pair(store, node.getId()), sands);
            nodeIdStoreInitialized.put(new Pair(store, node.getId()), false);

            remoteStoreDefs = adminClient.getRemoteStoreDefList(node.getId()).getValue();

        }

        boolean foundStore = false;

        for(StoreDefinition remoteStoreDef: remoteStoreDefs) {
            if(remoteStoreDef.getName().equals(store)) {
                RoutingStrategyFactory factory = new RoutingStrategyFactory();
                RoutingStrategy storeRoutingStrategy = factory.updateRoutingStrategy(remoteStoreDef,
                                                                                     adminClient.getAdminClientCluster());

                storeToRoutingStrategy.put(store, storeRoutingStrategy);
                foundStore = true;
                break;
            }
        }
        if(!foundStore) {
            throw new VoldemortException("Store Name not found on the cluster");

        }

    }

    /**
     ** key - The key
     * 
     * value - The value
     * 
     * storeName takes an additional storename as a parameter
     **/
    @SuppressWarnings({ "unchecked", "rawtypes", "unused" })
    public void streamingPut(ByteArray key, Versioned<byte[]> value, String storeName) {

        // If store does not exist in the stores list
        // add it and checkout a socket
        if(!storeNames.contains(storeName)) {
            addStoreToSession(storeName);
        }

        if(MARKED_BAD)
            throw new VoldemortException("Cannot stream more entries since Recovery Callback Failed!");

        List<Node> nodeList = storeToRoutingStrategy.get(storeName).routeRequest(key.get());

        // sent the k/v pair to the nodes
        for(Node node: nodeList) {

            VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                  .setKey(ProtoUtils.encodeBytes(key))
                                                                                  .setVersioned(ProtoUtils.encodeVersioned(value))
                                                                                  .build();

            VAdminProto.UpdatePartitionEntriesRequest.Builder updateRequest = VAdminProto.UpdatePartitionEntriesRequest.newBuilder()
                                                                                                                       .setStore(storeName)
                                                                                                                       .setPartitionEntry(partitionEntry);

            DataOutputStream outputStream = nodeIdStoreToOutputStreamRequest.get(new Pair(storeName,
                                                                                          node.getId()));
            try {
                if(nodeIdStoreInitialized.get(new Pair(storeName, node.getId()))) {
                    ProtoUtils.writeMessage(outputStream, updateRequest.build());
                } else {
                    ProtoUtils.writeMessage(outputStream,
                                            VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                             .setType(VAdminProto.AdminRequestType.UPDATE_PARTITION_ENTRIES)
                                                                             .setUpdatePartitionEntries(updateRequest)
                                                                             .build());
                    outputStream.flush();
                    nodeIdStoreInitialized.put(new Pair(storeName, node.getId()), true);

                }

                entriesProcessed++;

            } catch(IOException e) {
                Future future = streamingresults.submit(recoveryCallback);
                try {
                    future.get();

                } catch(InterruptedException e1) {
                    MARKED_BAD = true;
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                } catch(ExecutionException e1) {
                    MARKED_BAD = true;
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                }

                e.printStackTrace();
            }

        }

        if(entriesProcessed == BATCH_SIZE) {
            entriesProcessed = 0;
            newBatch = true;

            for(Node node: adminClient.getAdminClientCluster().getNodes()) {

                boolean nodeUsed = false; // check if any data was sent at all
                                          // to this node

                for(String store: storeNames) {
                    if(!nodeIdStoreInitialized.get(new Pair(store, node.getId())))
                        continue;
                    nodeUsed = true;
                    nodeIdStoreInitialized.put(new Pair(store, node.getId()), false);

                    DataOutputStream outputStream = nodeIdStoreToOutputStreamRequest.get(new Pair(store,
                                                                                                  node.getId()));

                    try {
                        ProtoUtils.writeEndOfStream(outputStream);
                        outputStream.flush();
                        DataInputStream inputStream = nodeIdStoreToInputStreamRequest.get(new Pair(store,
                                                                                                   node.getId()));
                        VAdminProto.UpdatePartitionEntriesResponse.Builder updateResponse = ProtoUtils.readToBuilder(inputStream,
                                                                                                                     VAdminProto.UpdatePartitionEntriesResponse.newBuilder());
                        if(updateResponse.hasError()) {
                            Future future = streamingresults.submit(recoveryCallback);
                            try {
                                future.get();

                            } catch(InterruptedException e1) {
                                // TODO Auto-generated catch block
                                e1.printStackTrace();
                            } catch(ExecutionException e1) {
                                // TODO Auto-generated catch block
                                e1.printStackTrace();
                            }
                        }
                        Future future = streamingresults.submit(checkpointCallback);
                        try {
                            future.get();

                        } catch(InterruptedException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        } catch(ExecutionException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }

                    } catch(IOException e) {
                        Future future = streamingresults.submit(recoveryCallback);
                        try {
                            future.get();

                        } catch(InterruptedException e1) {
                            MARKED_BAD = true;
                            e1.printStackTrace();
                            throw new VoldemortException("Recovery Callback failed");
                        } catch(ExecutionException e1) {
                            MARKED_BAD = true;
                            e1.printStackTrace();
                            throw new VoldemortException("Recovery Callback failed");
                        }

                        e.printStackTrace();
                    }
                }

            }
        }

        throttler.maybeThrottle(1);

    }

    /**
     * Close the streaming session Flush all n/w buffers and call the commit
     * callback
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void closeStreamingSessions() {

        for(Node node: adminClient.getAdminClientCluster().getNodes()) {

            boolean nodeUsed = false; // check if any data was sent at all
            // to this node

            for(String store: storeNames) {
                if(!nodeIdStoreInitialized.get(new Pair(store, node.getId())))
                    continue;
                nodeUsed = true;
                nodeIdStoreInitialized.put(new Pair(store, node.getId()), false);

                DataOutputStream outputStream = nodeIdStoreToOutputStreamRequest.get(new Pair(store,
                                                                                              node.getId()));

                try {
                    ProtoUtils.writeEndOfStream(outputStream);
                    outputStream.flush();
                    DataInputStream inputStream = nodeIdStoreToInputStreamRequest.get(new Pair(store,
                                                                                               node.getId()));
                    VAdminProto.UpdatePartitionEntriesResponse.Builder updateResponse = ProtoUtils.readToBuilder(inputStream,
                                                                                                                 VAdminProto.UpdatePartitionEntriesResponse.newBuilder());
                    if(updateResponse.hasError()) {
                        Future future = streamingresults.submit(recoveryCallback);
                        try {
                            future.get();

                        } catch(InterruptedException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        } catch(ExecutionException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                    }
                    Future future = streamingresults.submit(checkpointCallback);
                    try {
                        future.get();

                    } catch(InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    } catch(ExecutionException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }

                } catch(IOException e) {
                    Future future = streamingresults.submit(recoveryCallback);
                    try {
                        future.get();

                    } catch(InterruptedException e1) {
                        MARKED_BAD = true;
                        e1.printStackTrace();
                        throw new VoldemortException("Recovery Callback failed");
                    } catch(ExecutionException e1) {
                        MARKED_BAD = true;
                        e1.printStackTrace();
                        throw new VoldemortException("Recovery Callback failed");
                    }

                    e.printStackTrace();
                }
            }

        }

        cleanupSessions();

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void cleanupSessions() {
        for(String store: storeNames) {

            for(Node node: adminClient.getAdminClientCluster().getNodes()) {

                SocketAndStreams sands = nodeIdStoreToSocketAndStreams.get(new Pair(store,
                                                                                    node.getId()));
                close(sands.getSocket());
                SocketDestination destination = nodeIdStoreToSocketRequest.get(new Pair(store,
                                                                                        node.getId()));
                streamingSocketPool.checkin(destination, sands);
            }

        }

        cleanedUp = true;
    }

}
