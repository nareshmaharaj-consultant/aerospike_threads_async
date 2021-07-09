import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.async.Monitor;

import java.util.concurrent.atomic.AtomicLong;

public class RunnableConnectionExample
{

    static AtomicLong start = new AtomicLong(0);
    static int numberOfClients = 10;
    static int numberOfRecords = 1000;
    static int startKeyFrom = 0;
    static int port = 3000;

    static Host[] hosts = new Host[] {
            new Host("127.0.0.1", port)
    };

    static String namespace = "insurance";
    static String set = "exp1";

    static Monitor monitor = new Monitor();

    /*
        java -jar async.jar threads records "host1,host2,host3" ns set"
        java -jar async.jar 100 1000 "10.0.0.61,10.0.1.33,10.0.2.147" test set1"
     */
    public static void main(String [] args)
    {
        if ( args.length == 1 ) {
            System.out.println(
                    "e.g. java -jar async.jar threads records \"host1,host2,host3\" ns set\n" +
                    "e.g. java -jar async.jar 100 1000 \"10.0.0.61,10.0.1.33,10.0.2.147\" test set1");
            System.exit(0);
        }
        if ( args.length >= 2 ) {
            numberOfClients = Integer.parseInt(args[0]);
            numberOfRecords = Integer.parseInt(args[1]);
        }
        if ( args.length >= 3 ) {

            String [] listOfIps = args[2].split(",");
            Host[] tmpHost = new Host[listOfIps.length];
            for (int i = 0; i < listOfIps.length; i++) {
                tmpHost[i] =  new Host(listOfIps[i], port);
            }
            hosts = tmpHost;
        }

        if ( args.length >= 4 ) {
            namespace = args[3];
        }

        if ( args.length >= 5 ) {
            set = args[4];
        }

        if ( args.length >= 6 ) {
            startKeyFrom = Integer.parseInt(args[5]);
        }

        int numberOfRecordsPerThread = numberOfRecords / numberOfClients;
        long startTime = System.currentTimeMillis();

        System.out.println(
                "version 1\n" +
                "NumberOfClient Threads:" + numberOfClients +
                "\nNumberOfRecords to Insert:" + numberOfRecords +
                "\nStarting key:" + startKeyFrom
        );
        // System.exit(0);

        for (int i = 0; i < numberOfClients; i++ )
        {
            RunnableConnection R1 =
                    new RunnableConnection(
                            "Aerospike Connection: ".concat(Integer.toString(i)),
                            numberOfRecordsPerThread,
                            i * numberOfRecordsPerThread + startKeyFrom,
                            monitor,
                            namespace,
                            set
            );
            R1.start();
        }
        monitor.waitTillComplete();
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        System.out.println( "> TimeTaken " + ( timeTaken / 1 ) + " ms for " + numberOfRecords + " records.");
    }
}

class RunnableConnection implements Runnable {

    private final String namespace;
    private final String set;
    private int numberOfDocuments;
    private int startKey;
    private Thread t;
    private String threadName;
    AerospikeClient client;
    Monitor monitor;

    RunnableConnection(String name, int numOfRecords, int startKey, Monitor monitor, String namespace, String set) {
        threadName = name;
        this.client = new AerospikeClient(
                null, RunnableConnectionExample.hosts)
        ;
        this.numberOfDocuments = numOfRecords;
        this.startKey = startKey;
        this.monitor = monitor;
        this.namespace = namespace;
        this.set = set;
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

        Key key = new Key(namespace, set, keyIndex);
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
