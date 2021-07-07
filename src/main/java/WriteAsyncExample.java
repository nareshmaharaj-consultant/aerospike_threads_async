
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;

public final class WriteAsyncExample {

    static long startTime = System.currentTimeMillis();


    public static void main(String[] args) {
        try {
            WriteAsyncExample test = new WriteAsyncExample();
            test.runTest();

            long endTime = System.currentTimeMillis();
            long timeTaken = endTime - startTime;
            System.out.println( "> TimeTaken is " + ( timeTaken / 1000 ) + " seconds for " + recordMax + " docs");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private AerospikeClient client;
    private EventLoops eventLoops;
    private final Monitor monitor = new Monitor();
    private final AtomicInteger recordCount = new AtomicInteger();
    private final int maxCommandsInProcess = 10;
    private static final int recordMax = 100000;
    private final int writeTimeout = 5000;
    private final int eventLoopSize;
    private final int concurrentMax;

    public WriteAsyncExample() {
        // Allocate an event loop for each cpu core.
        eventLoopSize = Runtime.getRuntime().availableProcessors();

        // Set total concurrent commands for all event loops.
        concurrentMax = eventLoopSize * maxCommandsInProcess;
    }

    public void runTest() throws AerospikeException, Exception {
        EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.minTimeout = writeTimeout;

        // This application uses it's own external throttle (Start with concurrentMax
        // commands and only start one new command after previous command completes),
        // so setting EventPolicy maxCommandsInProcess is not necessary.
//         eventPolicy.maxCommandsInProcess = maxCommandsInProcess;

        // Direct NIO
        eventLoops = new NioEventLoops(eventPolicy, eventLoopSize);

        // Netty NIO
        // EventLoopGroup group = new NioEventLoopGroup(eventLoopSize);
        // eventLoops = new NettyEventLoops(eventPolicy, group);

        // Netty epoll (Linux only)
        // EventLoopGroup group = new EpollEventLoopGroup(eventLoopSize);
        // eventLoops = new NettyEventLoops(eventPolicy, group);

        try {
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.eventLoops = eventLoops;

            // maxConnsPerNode needs to be increased from default (300)
            // if eventLoopSize >= 8.
            clientPolicy.maxConnsPerNode = concurrentMax;

            clientPolicy.writePolicyDefault.setTimeout(writeTimeout);

            client = new AerospikeClient(clientPolicy, "localhost", 3000);

            try {
                writeRecords();
                monitor.waitTillComplete();
                // System.out.println(" > Records written: " + recordCount.get());
            } finally {
                client.close();
            }
        } finally {
            eventLoops.close();
        }
    }

    private void writeRecords() {
        // Write exactly concurrentMax commands to seed event loops.
        // Distribute seed commands across event loops.
        // A new command will be initiated after each command completion in WriteListener.
        for (int i = 1; i <= concurrentMax; i++) {
            EventLoop eventLoop = eventLoops.next();
            writeRecord(eventLoop, new AWriteListener(eventLoop), i);
        }
    }

    private void writeRecord(EventLoop eventLoop, WriteListener listener, int keyIndex) {

        // System.out.println( "writeRecord:" +  recordCount.get() );
        double r1 =  Math.ceil( Math.random() * recordMax );
        double r2 = Math.ceil( Math.random() * recordMax );
        int rand1 = (int)r1;
        int rand2 = (int)r2;

        Key key = new Key("insurance", "exp1", rand1);
        Bin bin = new Bin("bin", keyIndex);

        Bin binType = new Bin("type", "weather");
        Bin binTemp = new Bin("value", rand2);
        Bin binMeasure = new Bin("measure", "celcius");

        client.put(eventLoop, listener, null, key, bin, binType, binTemp, binMeasure );
    }

    private class AWriteListener implements WriteListener
    {
        private final EventLoop eventLoop;

        public AWriteListener(EventLoop eventLoop) {
            this.eventLoop = eventLoop;
        }

        @Override
        public void onSuccess(Key key) {
            try {
                int count = recordCount.incrementAndGet();

                // Stop if all records have been written.
                if (count >= recordMax) {
                    monitor.notifyComplete();
                    return;
                }

                if (count % 10000 == 0) {
                    // System.out.println("Records written: " + count);
                }

                // Issue one new command if necessary.
                int keyIndex = concurrentMax + count;
                if (keyIndex <= recordMax) {
                    // Write next record on same event loop.
                    writeRecord(eventLoop, this, keyIndex);
                }
            } catch (Exception e) {
                e.printStackTrace();
                monitor.notifyComplete();
            }
        }

        @Override
        public void onFailure(AerospikeException e) {
            e.printStackTrace();
            monitor.notifyComplete();
        }
    }
}
