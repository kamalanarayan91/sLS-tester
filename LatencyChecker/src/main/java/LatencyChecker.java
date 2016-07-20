import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.rabbitmq.client.Consumer;
import org.apache.commons.lang.SerializationUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kamala on 6/1/16.
 */
public class LatencyChecker extends RMQEndPoint implements Consumer
{
    private CheckerThreadPool checkerThreadPool;
    public static final String SUBSCRIBEQUEUE = "Q2";

    public static long N = 500 ;
    public static long M = 800 ;
    public static double T = 120;

    /***Change this for Index****/
    public static final String INDEX= "/n500_mean2_timeout30";
    public static final String INDEXENDPOINT = "/latency";
    /***************/

    public static String DATASTOREENDPOINT = "";
    public static String MAPPINGENDPOINT = "";

    public static final String QUERY= "/perfsonar/records/_search";

    public static int MAXWAITTIME = 2000 * 60;

    public static String SLSCACHEENDPOINT= "";

    static {
        System.setProperty("org.apache.commons.logging.Log",
                "org.apache.commons.logging.impl.NoOpLog");
    }


    public static ConcurrentHashMap<String,Date> uriExpiryMap = new ConcurrentHashMap<String, Date>();

    public static AtomicInteger nextRequest = new AtomicInteger(2);




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
            if(uriExpiryMap.get(uri)==null)
            {
                uriExpiryMap.put(uri, expiryDate);
            }
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
        if(message.getMessageType().equals("REGISTER"))
        {
            if(message.getIsStored()== true && LatencyChecker.uriExpiryMap.get(message.getUri())==null)
            {
                LatencyChecker.uriExpiryMap.put(message.getUri() , message.getExpiresDate());

            }

        }
        checkerThreadPool.checkLatency(message);
    }

    public void handleCancel(String consumerTag) {}
    public void handleCancelOk(String consumerTag) {}
    public void handleRecoverOk(String consumerTag) {}
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException arg1) {}

    public static void printHelp()
    {
        System.out.println("java LatencyChecker <M> <N> <T> <SlsCacheHostName> <FinalDataStoreHostName>");
        System.out.println("example: 500 800 2 ps-cache-west.es.net sowmya-dev-vm.es.net" );
        System.out.println("thread wait time should be in minutes");
    }

    /**
     * Initializes the static variable
     * @param args
     */
    public static void initialize(String[] args)
    {

        System.out.println("M:"+M +" N: "+ N + " T:" + T);
        M = Long.parseLong(args[0]);
        N = Long.parseLong(args[1]);
        T = Long.parseLong(args[2]);
        SLSCACHEENDPOINT = "http://"+args[3]+":9200"+ QUERY;
        MAPPINGENDPOINT = "http://"+args[4]+":9200"+ INDEX;
        DATASTOREENDPOINT = "http://"+args[4]+":9200"+ INDEX+INDEXENDPOINT;

        System.out.println("Cache: "+ SLSCACHEENDPOINT);
        System.out.println("DataStore:"+ DATASTOREENDPOINT);
    }

    /**
     * Creates an index and mapping for further use.
     * Ignores if the index already exists.
     */
    public static void initializeElasticSearch()
    {
       String mappingJSONString=  "{\"mappings\": " +
                                        "{\"latency\": " +
                                            "{\"properties\":         " +
                                                "{\"latency\":  {\"type\":\"long\"},       " +
                                                "\"M\":{\"type\":\"long\"}," +
                                                "\"N\":{\"type\":\"long\"}," +
                                                "\"T\":{\"type\":\"long\"}," +
                                                "\"creationTime:\":{\"type\":\"date\",\"format\":\"strict_date_optional_time||epoch_millis\"}," +
                                                "\"expires\":{\"type\":\"date\",\"format\":\"strict_date_optional_time||epoch_millis\"}," +
                                                "\"messageType\":{\"type\":\"string\",\"index\":\"not_analyzed\"}," +
                                                "\"uri\":{\"type\":\"string\",\"index\":\"not_analyzed\"}," +
                                                "\"result\":{\"type\":\"string\",\"index\":\"not_analyzed\"}" +
                                                "}}}}";


        Gson gson = new Gson();
        String json = mappingJSONString;

        //send to ES store
        try
        {

            CloseableHttpClient httpClient    = HttpClients.createDefault();
            HttpPost post          = new HttpPost(MAPPINGENDPOINT);
            post.setHeader("Content-type", "application/json");
            StringEntity stringEntity = new StringEntity(json);
            post.setEntity(stringEntity);

            CloseableHttpResponse response = httpClient.execute(post);

            // get back response.
            if(response.getStatusLine().getStatusCode() == 200)
            {
                System.out.println("Index Created!");
            }
            else
            {
                System.err.println("Status response from Data Store: " + response.getStatusLine().getStatusCode());
                System.err.println("The Index might already Exist in the data Store. Please delete it if not needed");

            }

            //clean up
            response.close();
            post.releaseConnection();
            httpClient.close();

        }
        catch(IOException e)
        {
            System.out.println("There's an error in the sending the mapping request");

        }
    }


    public static void main(String[] args)
    {
        if(args.length != 5)
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
