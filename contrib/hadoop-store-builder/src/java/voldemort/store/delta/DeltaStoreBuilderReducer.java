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

package voldemort.store.delta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import voldemort.client.protocol.admin.StreamingClient;
import voldemort.client.protocol.admin.StreamingClientConfig;
import voldemort.store.readonly.disk.KeyValueWriter;
import voldemort.store.readonly.mr.AbstractStoreBuilderConfigurable;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

/**
 * Take key md5s and value bytes and build a read-only store from these values
 */
@SuppressWarnings("deprecation")
public class DeltaStoreBuilderReducer extends AbstractStoreBuilderConfigurable implements
        Reducer<BytesWritable, BytesWritable, Text, Text> {

    String keyValueWriterClass;
    @SuppressWarnings("rawtypes")
    KeyValueWriter writer;

    StreamingClient streamer;

    /**
     * Reduce should get sorted MD5 of Voldemort key ( either 16 bytes if saving
     * keys is disabled, else 8 bytes ) as key and for value (a) node-id,
     * partition-id, value - if saving keys is disabled (b) node-id,
     * partition-id, replica-type, [key-size, value-size, key, value]* if saving
     * keys is enabled
     */
    @SuppressWarnings("unchecked")
    public void reduce(BytesWritable key,
                       Iterator<BytesWritable> iterator,
                       OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {

        byte[] keyBytes = key.get();
        byte[] originalkey = new byte[keyBytes.length - 6];
        // arraycopy(Object src, int srcPos, Object dest, int destPos, int
        // length)
        System.arraycopy(keyBytes, 6, originalkey, 0, keyBytes.length - 6);
        while(iterator.hasNext()) {
            BytesWritable writable = iterator.next();
            Versioned<byte[]> outputValue = Versioned.value(writable.get());

            streamer.streamingPut(new ByteArray(originalkey), outputValue, "delta-store");
        }

    }

    @Override
    public void configure(JobConf job) {

        super.configure(job);
        String url = "tcp://localhost:6666";

        Props property = new Props();
        property.put("streaming.platform..bootstrapURL", url);
        StreamingClientConfig config = new StreamingClientConfig(property);
        config.setBootstrapURL(url);
        streamer = new StreamingClient(config);

        Callable<Integer> cpCallable = new CheckpointCallable();
        Callable<Integer> rbCallable = new RollbackCallable();
        List<String> stores = new ArrayList();
        streamer.initStreamingSessions(stores, cpCallable, rbCallable, true);
    }

    public static int toInt(byte[] bytes, int offset) {
        int ret = 0;
        for(int i = 0; i < 4 && i + offset < bytes.length; i++) {
            ret <<= 8;
            ret |= (int) bytes[i] & 0xFF;
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
        streamer.closeStreamingSessions();
    }
}

class CheckpointCallable implements Callable {

    public CheckpointCallable() {

    }

    public Integer call() {
        System.out.println("checkpoint!");
        return 1;
    }
}

class RollbackCallable implements Callable {

    public RollbackCallable() {

    }

    public Integer call() {
        System.out.println("rollback!");
        return 1;
    }
}
