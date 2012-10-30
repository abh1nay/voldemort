/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

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

/**
 * 
 * @author anagpal
 * 
 *         The streaming API allows for send events into voldemort stores in the
 *         async fashion. All the partition and replication logic will be taken
 *         care of internally.
 * 
 *         The users is expected to provide two callbacks, one for performing
 *         period checkpoints and one for recovering the streaming process from
 *         the last checkpoint.
 * 
 *         NOTE: The API is not thread safe, since if multiple threads use this
 *         API we cannot make any guarantees about correctness of the
 *         checkpointing mechanism.
 * 
 *         Right now we expect this to used by a single thread per data source
 * 
 */
public class StreamingClient {

    private static final Logger logger = Logger.getLogger(StreamingClient.class);
    @SuppressWarnings("rawtypes")
    private Callable checkpointCallback = null;
    @SuppressWarnings("rawtypes")
    private Callable recoveryCallback = null;
    private boolean allowMerge = false;

    private SocketPool streamingSocketPool;

    List<StoreDefinition> remoteStoreDefs;
    protected RoutingStrategy routingStrategy;

    boolean newBatch;
    boolean cleanedUp = false;

    boolean isMultiSession;
    ExecutorService streamingresults;

    // Every batch size we commit
    private static int BATCH_SIZE;

    // we have to throttle to a certain qps
    private static int THROTTLE_QPS;
    private int entriesProcessed;

    // In case a Recovery callback fails we got to stop it from getting any
    // worse
    // so we mark the session as bad and dont take any more requests
    private static boolean MARKED_BAD = false;

    protected EventThrottler throttler;

    AdminClient adminClient;
    AdminClientConfig adminClientConfig;

    String bootstrapURL;

    // Data structures for the streaming maps from Pair<Store, Node Id> to
    // Resource

    private HashMap<String, RoutingStrategy> storeToRoutingStrategy;
    private HashMap<Pair<String, Integer>, Boolean> nodeIdStoreInitialized;

    private HashMap<Pair<String, Integer>, SocketDestination> nodeIdStoreToSocketRequest;
    private HashMap<Pair<String, Integer>, DataOutputStream> nodeIdStoreToOutputStreamRequest;
    private HashMap<Pair<String, Integer>, DataInputStream> nodeIdStoreToInputStreamRequest;

    private HashMap<Pair<String, Integer>, SocketAndStreams> nodeIdStoreToSocketAndStreams;

    private List<String> storeNames;

    private final static int MAX_STORES_PER_SESSION = 100;

    public StreamingClient(StreamingClientConfig config) {
        this.bootstrapURL = config.getBootstrapURL();
        BATCH_SIZE = config.getBatchSize();
        THROTTLE_QPS = config.getThrottleQPS();

    }

    /**
     ** 
     * @param store - the name of the store to be streamed to
     * 
     * @param checkpointCallback - the callback that allows for the user to
     *        record the progress, up to the last event delivered. This callable
     *        would be invoked every so often internally.
     * 
     * @param recoveryCallback - the callback that allows the user to rewind the
     *        upstream to the position recorded by the last complete call on
     *        checkpointCallback whenever an exception occurs during the
     *        streaming session.
     * 
     * @param allowMerge - whether to allow for the streaming event to be merged
     *        with online writes. If not, all online writes since the completion
     *        of the last streaming session will be lost at the end of the
     *        current streaming session.
     **/
    @SuppressWarnings({ "rawtypes", "unused", "unchecked" })
    public void initStreamingSession(String store,
                                     Callable checkpointCallback,
                                     Callable recoveryCallback,
                                     boolean allowMerge) {

        // internally call initsessions with a single store
        List<String> stores = new ArrayList();
        stores.add(store);
        initStreamingSessions(stores, checkpointCallback, recoveryCallback, allowMerge);

    }

    /**
     * A Streaming Put call
     ** 
     * @param key - The key
     * 
     * @param value - The value
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void streamingPut(ByteArray key, Versioned<byte[]> value) {

        if(MARKED_BAD) {
            logger.error("Cannot stream more entries since Recovery Callback Failed!");
            throw new VoldemortException("Cannot stream more entries since Recovery Callback Failed!");
        }

        for(String store: storeNames) {
            streamingPut(key, value, store);
        }

    }

    /**
     ** 
     * @param resetCheckpointCallback - the callback that allows for the user to
     *        clean up the checkpoint at the end of the streaming session so a
     *        new session could, if necessary, start from 0 position.
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void closeStreamingSession(Callable resetCheckpointCallback) {

        closeStreamingSessions(resetCheckpointCallback);

    }

    /**
     * Close the streaming session Flush all n/w buffers and call the commit
     * callback
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void closeStreamingSession() {

        closeStreamingSessions();

    }

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }

    @Override
    protected void finalize() {

        if(!cleanedUp) {
            cleanupSessions();
        }

    }

    /**
     ** 
     * @param stores - the list of name of the stores to be streamed to
     * 
     * 
     * @param checkpointCallback - the callback that allows for the user to
     *        record the progress, up to the last event delivered. This callable
     *        would be invoked every so often internally.
     * 
     * @param recoveryCallback - the callback that allows the user to rewind the
     *        upstream to the position recorded by the last complete call on
     *        checkpointCallback whenever an exception occurs during the
     *        streaming session.
     * 
     * @param allowMerge - whether to allow for the streaming event to be merged
     *        with online writes. If not, all online writes since the completion
     *        of the last streaming session will be lost at the end of the
     *        current streaming session.
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void initStreamingSessions(List<String> stores,
                                      Callable checkpointCallback,
                                      Callable recoveryCallback,
                                      boolean allowMerge) {

        logger.info("Initializing a streaming session");
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
                logger.error("Store Name not found on the cluster");
                throw new VoldemortException("Store Name not found on the cluster");

            }
        }

    }

    /**
     * Add another store destination to an existing streaming session
     * 
     * 
     * @param store: the name of the store to stream to
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
            logger.error("Store Name not found on the cluster");
            throw new VoldemortException("Store Name not found on the cluster");

        }

    }

    /**
     ** 
     * @param key - The key
     * 
     * @param value - The value
     * 
     * @param storeName takes an additional store name as a parameter
     **/
    @SuppressWarnings({ "unchecked", "rawtypes", "unused" })
    public void streamingPut(ByteArray key, Versioned<byte[]> value, String storeName) {

        // If store does not exist in the stores list
        // add it and checkout a socket
        if(!storeNames.contains(storeName)) {
            addStoreToSession(storeName);
        }

        if(MARKED_BAD) {
            logger.error("Cannot stream more entries since Recovery Callback Failed!");
            throw new VoldemortException("Cannot stream more entries since Recovery Callback Failed!");
        }

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
                logger.warn("Invoking the Recovery Callback");
                Future future = streamingresults.submit(recoveryCallback);
                try {
                    future.get();

                } catch(InterruptedException e1) {
                    MARKED_BAD = true;
                    logger.error("Recovery Callback failed");
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                } catch(ExecutionException e1) {
                    MARKED_BAD = true;
                    logger.error("Recovery Callback failed");
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                }

                e.printStackTrace();
            }

        }

        if(entriesProcessed == BATCH_SIZE) {
            entriesProcessed = 0;
            newBatch = true;

            logger.info("Trying to commit to Voldemort");
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
                            logger.warn("Invoking the Recovery Callback");
                            Future future = streamingresults.submit(recoveryCallback);
                            try {
                                future.get();

                            } catch(InterruptedException e1) {
                                MARKED_BAD = true;
                                logger.error("Recovery Callback failed");
                                e1.printStackTrace();
                                throw new VoldemortException("Recovery Callback failed");
                            } catch(ExecutionException e1) {
                                MARKED_BAD = true;
                                logger.error("Recovery Callback failed");
                                e1.printStackTrace();
                                throw new VoldemortException("Recovery Callback failed");
                            }
                        } else {
                            logger.info("Commit successful");
                            logger.info("calling checkpoint callback");
                            Future future = streamingresults.submit(checkpointCallback);
                            try {
                                future.get();

                            } catch(InterruptedException e1) {

                                logger.warn("Checkpoint callback failed!");
                                e1.printStackTrace();
                            } catch(ExecutionException e1) {
                                logger.warn("Checkpoint callback failed!");
                                e1.printStackTrace();
                            }
                        }

                    } catch(IOException e) {

                        logger.warn("Invoking the Recovery Callback");
                        Future future = streamingresults.submit(recoveryCallback);
                        try {
                            future.get();

                        } catch(InterruptedException e1) {
                            MARKED_BAD = true;
                            logger.error("Recovery Callback failed");
                            e1.printStackTrace();
                            throw new VoldemortException("Recovery Callback failed");
                        } catch(ExecutionException e1) {
                            MARKED_BAD = true;
                            logger.error("Recovery Callback failed");
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
     ** 
     * @param resetCheckpointCallback - the callback that allows for the user to
     *        clean up the checkpoint at the end of the streaming session so a
     *        new session could, if necessary, start from 0 position.
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void closeStreamingSessions(Callable resetCheckpointCallback) {

        closeStreamingSessions();

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
    public void closeStreamingSessions() {

        logger.info("closing the Streaming session");
        logger.info("Trying to commit to Voldemort");
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

                        logger.warn("Invoking Recovery Callback");
                        Future future = streamingresults.submit(recoveryCallback);
                        try {
                            future.get();

                        } catch(InterruptedException e1) {
                            MARKED_BAD = true;
                            logger.error("Recovery Callback failed");
                            e1.printStackTrace();
                            throw new VoldemortException("Recovery Callback failed");
                        } catch(ExecutionException e1) {
                            MARKED_BAD = true;
                            logger.error("Recovery Callback failed");
                            e1.printStackTrace();
                            throw new VoldemortException("Recovery Callback failed");
                        }
                    } else {

                        logger.info("Commit successful");
                        logger.info("calling checkpoint callback");
                        Future future = streamingresults.submit(checkpointCallback);
                        try {
                            future.get();

                        } catch(InterruptedException e1) {
                            logger.warn("Checkpoint callback failed!");
                            e1.printStackTrace();
                        } catch(ExecutionException e1) {
                            logger.warn("Checkpoint callback failed!");
                            e1.printStackTrace();
                        }

                    }

                } catch(IOException e) {
                    logger.warn("Invoking Recovery Callback");
                    Future future = streamingresults.submit(recoveryCallback);
                    try {
                        future.get();

                    } catch(InterruptedException e1) {
                        MARKED_BAD = true;
                        logger.error("Recovery Callback failed");
                        e1.printStackTrace();
                        throw new VoldemortException("Recovery Callback failed");
                    } catch(ExecutionException e1) {
                        MARKED_BAD = true;
                        logger.error("Recovery Callback failed");
                        e1.printStackTrace();
                        throw new VoldemortException("Recovery Callback failed");
                    }

                    e.printStackTrace();
                }
            }

        }

        cleanupSessions();

    }

    /**
     * Helper method to Close all open socket connections and checkin back to
     * the pool
     */

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void cleanupSessions() {

        logger.info("Performing cleanup");
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
