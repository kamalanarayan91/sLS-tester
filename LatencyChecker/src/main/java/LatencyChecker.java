import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.rabbitmq.client.Consumer;
import org.apache.commons.lang.SerializationUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kamala on 6/1/16.
 */
public class LatencyChecker extends RMQEndPoint implements Consumer
{
    private CheckerThreadPool checkerThreadPool;
    public static final String SUBSCRIBEQUEUE = "Q2";

    public static final String INDEXENDPOINT = "/data/latency";
    public static String DATASTOREENDPOINT = "http://localhost:9200/data100/latency";

    //public static final String QUERY= "/perfsonar/_search?q=uri:";
    public static final String QUERY= "/perfsonar/records/_search";

    public static int MAXWAITTIME = 2000 * 60;

    public static String SLSCACHEENDPOINT= "";

    static {
        System.setProperty("org.apache.commons.logging.Log",
                "org.apache.commons.logging.impl.NoOpLog");
    }


    public static ConcurrentHashMap<String,Date> uriExpiryMap = new ConcurrentHashMap<String, Date>();


    public static long N = -1;
    public static long M = -1;
    public static double T = -1;


    public static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    /*Constructor*/
    public LatencyChecker()
    {
        super(SUBSCRIBEQUEUE);
        checkerThreadPool = new CheckerThreadPool();
    }

    /**
     * Stores in map
     * */
    public static synchronized void storeInfo(String uri, String expiresDate)
    {
        try
        {
            Date expiryDate = dateFormat.parse(expiresDate);
            uriExpiryMap.put(uri, expiryDate);
        }
        catch(ParseException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Registers this object with rabbitmq as a consumer
     */
    public void startConsumer()
    {
        try
        {
            // start consuming messages. Auto acknowledge messages.
            channel.basicConsume(SUBSCRIBEQUEUE, true, this);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Called when consumer is registered.
     */
    public void handleConsumeOk(String consumerTag)
    {
        System.out.println("Consumer "+consumerTag +" registered");
    }

    /**
     * Called when new message is available.
     */
    public void handleDelivery(String consumerTag, Envelope env,
                               AMQP.BasicProperties props, byte[] body) throws IOException
    {
        LGMessage message = (LGMessage) SerializationUtils.deserialize(body);
       // System.out.println("message:"+ message.getMessageId() +" received" + "--" + message.getMessageType() + " uri:"
         //                       + message.getUri());
        checkerThreadPool.checkLatency(message);

    }

    public void handleCancel(String consumerTag) {}
    public void handleCancelOk(String consumerTag) {}
    public void handleRecoverOk(String consumerTag) {}
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException arg1) {}

    public static void printHelp()
    {
        System.out.println("java LatencyChecker <M> <N> <T> <SlsCacheHostName> <FinalDataStoreHostName> <maxthreadWaitTime>");
        System.out.println("example: java LatencyChecker 100 100 100  localhost 192.168.56.103 2" );
        System.out.println("thread wait time should be in minutes");
    }

    /**
     * Initializes the static variable
     * @param args
     */
    public static void initialize(String[] args)
    {
        M = Long.parseLong(args[0]);
        N = Long.parseLong(args[1]);
        T = Double.parseDouble(args[2]);
        SLSCACHEENDPOINT = "http://"+args[3]+":9200"+ QUERY;
        DATASTOREENDPOINT = "http://"+args[4]+":9200"+ INDEXENDPOINT;
        MAXWAITTIME = Integer.parseInt(args[5])*1000*60;
    }

    /**
     * Creates an index and mapping for further use.
     * Ignores if the index already exists.
     */
    public static void initializeElasticSearch()
    {

        /*
        * 	{"mappings":
    	{"latency":
        	{"properties":
            	{"Latency":
                	{"type":"double"},
                    	"M":{"type":"long"},
						"N":{"type":"long"},
                        "T":{"type":"double"},
                        "creationTime:":{"type":"date","format":"strict_date_optional_time||epoch_millis"},
                        "expires":{"type":"date","format":"strict_date_optional_time||epoch_millis"},
                        "messageType":{"type":"string","index":"not_analyzed"},
                        "latency":{"type":"long"},
                        "uri":{"type":"string","index":"not_analyzed"}}}}}
                        */

    }
    public static void main(String[] args)
    {
        if(args.length!=6)
        {
            printHelp();
            System.exit(-1);
        }

        initialize(args);

        LatencyChecker latencyChecker = new LatencyChecker();
        latencyChecker.initializeElasticSearch();
        latencyChecker.startConsumer();
    }
}
