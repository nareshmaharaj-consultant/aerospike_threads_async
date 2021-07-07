import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.Monitor;

import java.util.concurrent.atomic.AtomicLong;

public class RunnableConnectionExample
{
    static AtomicLong start = new AtomicLong(0);
    static int numberOfClients = 100;
    static int numberOfRecords = 100000;
    static Monitor monitor = new Monitor();

    public static void main(String [] args)
    {
        int numberOfRecordsPerThread = numberOfRecords / numberOfClients;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numberOfClients; i++ )
        {
            RunnableConnection R1 =
                    new RunnableConnection(
                            "Aerospike Connection: ".concat(Integer.toString(i)),
                            numberOfRecordsPerThread,
                            i * numberOfRecordsPerThread,
                            monitor
            );
            R1.start();
        }
        monitor.waitTillComplete();
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.println( "> TimeTaken is " + ( timeTaken / 1000 ) + " seconds for " + numberOfRecords + " records.");
    }
}

class RunnableConnection implements Runnable {

    private int numberOfDocuments;
    private int startKey;
    private Thread t;
    private String threadName;
    AerospikeClient client;
    Monitor monitor;

    RunnableConnection(String name, int numOfRecords, int startKey, Monitor monitor) {
        threadName = name;
        this.client = new AerospikeClient(null, "localhost", 3000);;
        this.numberOfDocuments = numOfRecords;
        this.startKey = startKey;
        this.monitor = monitor;
    }

    @Override
    public void run()
    {
        for (int i = startKey; i < (numberOfDocuments + startKey); i++) {
            writeRecord(i);
        }
        client.close();
        monitor.notifyComplete();
    }

    private void writeRecord(int keyIndex) {

        // System.out.println( "writeRecord:" +  recordCount.get() );
        double r1 =  Math.ceil( Math.random() * 100 );
        double r2 = Math.ceil( Math.random() * 1000 );
        int rand1 = (int)r1;
        int rand2 = (int)r2;

        Key key = new Key("insurance", "exp2", keyIndex);
        Bin bin = new Bin("bin", keyIndex);

        Bin binType = new Bin("val1", rand1);
        Bin binTemp = new Bin("val2", rand2);
        Bin binMeasure = new Bin("measure", "celcius");
        client.put(null, key, bin, binType, binTemp, binMeasure );
    }

    public void start () {
        // System.out.println("Starting " +  threadName );
        if (t == null) {
            t = new Thread (this, threadName);
            t.start ();
        }
    }
}
