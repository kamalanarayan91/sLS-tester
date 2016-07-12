import java.net.HttpURLConnection;
import java.net.URL;
import  java.util.*;
import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;


import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * Created by kamala on 6/6/16.
 */
public class DistributionLoadGenerator extends RMQEndPoint
{
    private double mean;
    private long lastExecutionTime = -1;

    private long interval=1100; // 1 second
    private AbstractIntegerDistribution distribution;
    private double ratio = 0.8; //90% register and 10% renew

    private static final String RENEW = "RENEW";
    private static final String REGISTER = "REGISTER";
    private static final String QUEUENAME = "Q2";
    private static final int FACTOR = 1000;

    public static final int TOTALENTRIES=10; // for NOW
    public ArrayList<HashMap<String,String>> dataList;
    public int currentRecordIndex;
    public int currentId;

    public static AtomicInteger keyIndex = new AtomicInteger(0);
    public static final int NUMENTRIES = 5000;
    public static AtomicInteger currentEntries = new AtomicInteger(0);


    public static final String sLSCoreHostName = "http://ps-cache.es.net:8090";
    public static final String pathName =  "/lookup/records";
    public static final int VALIDITY = 2;
    public Random rand;
    public static int counter = 0;
    public static int MEAN = 100;

    public HashMap<Integer,Record> uriMap;

    /**
     * Constructor
     **/
    public DistributionLoadGenerator(double mean)
    {
        super(QUEUENAME);
        this.mean = mean;
        distribution = new PoissonDistribution(mean);
        uriMap = new HashMap<Integer, Record>();
        dataList = new ArrayList<HashMap<String, String>>();
        rand = new Random();
    }

    /**
     * Gets the next delay
     * @return
     */
    public double getRandomNumber()
    {


        return  Math.log(1- rand.nextDouble())/(-mean);


    }
    /**
     * Generates random data
     */
    public void populateDataList()
    {
        for(int index=0; index < TOTALENTRIES; index++)
        {
            HashMap<String,String> dataMap = new HashMap<String, String>();

            for(int mapKey=1;mapKey<=10;mapKey++)
            {
                dataMap.put(Integer.toString(mapKey),Double.toString(Math.random()));
            }

            dataMap.put("type","Testing");
            dataList.add(dataMap);
        }
    }

    /**
     * Gets the next key.
     * @return the key for the hashmap
     */
    public static int getNextKey()
    {

        int key = keyIndex.getAndIncrement();
        if(key == NUMENTRIES)
        {
            keyIndex.set(0);
            key = 0;
        }

        return key % (NUMENTRIES);
    }

    /**
     * Gets a random uri in order to renew it with the sLs core node
     * @return
     */
    public Record getRandomRecord()
    {
        while(uriMap.size()==0)
        {
            /**
             *  Wait till there is atleast one entry in the map.
             *  Due to latency issues for register, this waiting is absolutely
             * required.
             **/
        }
        double key = Math.random() * uriMap.size();
        Double keyD = new Double(key);
        int intKey = keyD.intValue();
        return uriMap.get(intKey);
    }

    public Record putInfo(String uri,String expiresDate)
    {
        int key = getNextKey();
        Record record = new Record(uri,expiresDate);

        int currentNum = currentEntries.get();

        if(currentNum<NUMENTRIES)
        {
            uriMap.put(key, record);
            currentEntries.getAndIncrement();
            record.setIsStored(true);
        }
        else
        {
            record.setIsStored(false);
        }

        return record;

    }

    /**
     * Gets the type of request to generate based on a random number
     * @return
     */
    public String getRequestType()
    {
        double rand = Math.random();
        if(rand > ratio)
        {
            return RENEW;
        }

        return REGISTER;
    }

    /**
     * Send the request
     * */
    public void sendRequest(String requestType)
    {
        if(requestType.equals(REGISTER))
        {

            /**/
            HashMap<String,String> map = dataList.get(currentRecordIndex);

            //set Message parameters
            map.put("Id",Integer.toString(currentId));

            //handle register
            HashMap<String,String> msgDataMap =  map;

            HttpURLConnection httpcon = null;
            String url = sLSCoreHostName+pathName;
            Gson gson = new Gson();

            String data = gson.toJson(msgDataMap,HashMap.class);
            String result = null;

            try
            {
                //Connect
                httpcon = (HttpURLConnection) ((new URL(url).openConnection()));
                httpcon.setDoOutput(true);
                httpcon.setRequestProperty("Content-Type", "application/json");
                httpcon.setRequestProperty("Accept", "*/*");
                httpcon.setRequestProperty("Content-Length",Integer.toString(data.length()));
                httpcon.setRequestMethod("POST");
                httpcon.connect();


                //Write
                OutputStream os = httpcon.getOutputStream();
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
                writer.write(data);
                writer.close();
                os.close();

                //Read
                BufferedReader br = new BufferedReader(new InputStreamReader(httpcon.getInputStream(),"UTF-8"));

                String line = null;
                StringBuilder sb = new StringBuilder();

                while ((line = br.readLine()) != null)
                {
                    sb.append(line);

                }

                br.close();
                result = sb.toString();

                HashMap<String,String> map2 = gson.fromJson(result,HashMap.class);
                Record record = putInfo(map2.get("uri"),map2.get("expires"));

                //calculate created Time:
                Date expiryDate = record.getExpiresDate();
                Calendar cal = Calendar.getInstance();
                cal.setTime(expiryDate);
                cal.add(Calendar.HOUR, -1 * VALIDITY);
                Date successTime = cal.getTime();
                //
                //publish to queue for latencyChecker to consume
                LGMessage lgMessage = new LGMessage();
                lgMessage.setMessageId(currentId);
                lgMessage.setTimestamp(successTime);
                lgMessage.setUri(map2.get("uri"));
                lgMessage.setMessageType(LGMessage.REGISTER);
                lgMessage.setExpiresDate(record.getExpiresDate());
                lgMessage.setIsStored(record.getIsStored());

                //publishMessage(lgMessage);
                publish(lgMessage);



            }
            catch (UnsupportedEncodingException e)
            {
                e.printStackTrace();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            finally
            {

                if(httpcon!=null)
                {
                    httpcon.disconnect();
                }

            }
            /**/

            //increment record indices.
            currentId++;
            currentRecordIndex++;
            currentRecordIndex = currentRecordIndex%TOTALENTRIES;


        }
        else
        {
            if(uriMap.size()==0)
            {
                return;
            }

            try
            {
                // handle renew
                Record record = getRandomRecord();
                String uri = record.getUri();

                // get connections from database
                // get random db record
                // send renew record.
                String       postUrl       = sLSCoreHostName +"/" + uri;// put in your url
                Gson         gson          = new Gson();
                CloseableHttpClient httpClient    = HttpClients.createDefault();
                HttpPost     post          = new HttpPost(postUrl);
                post.setHeader("Content-type", "application/json");

                CloseableHttpResponse response = httpClient.execute(post);

                // get back response.
                if(response.getStatusLine().getStatusCode() == 200)
                {

                }
                else
                {
                    System.err.println("Status response: " + response.getStatusLine().getStatusCode());
                }



                //calculate created Time:
                Date expiryDate = record.getExpiresDate();
                Calendar cal = Calendar.getInstance();
                cal.setTime(expiryDate);
                cal.add(Calendar.HOUR, -1 * VALIDITY);
                Date successTime = cal.getTime();


                //send to tier 3
                //publish to queue for latencyChecker to consume
                LGMessage lgMessage = new LGMessage();
                lgMessage.setMessageId(currentId);
                lgMessage.setTimestamp(successTime);
                lgMessage.setUri(record.getUri());
                lgMessage.setMessageType(LGMessage.RENEW);
                lgMessage.setExpiresDate(record.getExpiresDate());

                //publish
                publish(lgMessage);

                //cleanup
                response.close();
                httpClient.close();
                currentId++;



            }
            catch(IOException e)
            {
                System.out.println("There's an error in the sending the http renew request");
                e.printStackTrace();

            }



        }

    }


    public void publish(LGMessage message)
    {

        try
        {

            channel.basicPublish("", QUEUENAME, null, SerializationUtils.serialize(message));
            System.out.println("Counter "+counter +" MessNumber:"+ message.getMessageId() + " type:" + message.getMessageType()  +" uri:"+ message.getUri() + " FINISHED");

        }
        catch(Exception e)
        {
            System.err.println("Error in serializing message");
        }



    }

    public static void main(String[] args)
    {
        long requestTime = 0;

        DistributionLoadGenerator distributionLoadGenerator = new DistributionLoadGenerator(MEAN);
        distributionLoadGenerator.populateDataList();

        while(true)
        {
            /*Sleep if one second hasn't passed */
            long sleepTime = 1000;
            long difference = sleepTime - requestTime;

            int numRequests = distributionLoadGenerator.distribution.sample();

            try
            {
                Thread.sleep(1000);
                counter++;
                System.out.println("Counter:"+counter +" num:"+ numRequests);

                if(counter==600)
                    System.exit(10000);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }


            long startTime = System.currentTimeMillis();
            for(int index=0;index<numRequests;index++)
            {
                String requestType = distributionLoadGenerator.getRequestType();
                distributionLoadGenerator.sendRequest(requestType);
            }
            long endTime = System.currentTimeMillis();

            requestTime = endTime - startTime;
        }
    }



}
