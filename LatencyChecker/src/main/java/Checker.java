import com.google.gson.Gson;


import net.sf.json.JSONArray;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import net.sf.json.JSONObject;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

/**
 * Created by kamala on 6/2/16.
 */
public class Checker implements Runnable
{

    private LGMessage message;
    private static final long DROP = -1;

    public static final int REGISTERLOOPTIME = 30000;//3 seconds
    private int executionCount;
    private static final int EXECUTIONLIMIT = 5;


    public Checker(LGMessage message)
    {
        executionCount= 0;
        this.message = message;
        Log log = LogFactory.getLog(JSONObject.class);

    }

    public void run()
    {


        String uri = message.getUri();
        long firstExecutionTime = System.currentTimeMillis();

        /*POST JSON
        {
            "query" : {
            "constant_score" : {
                "filter" : {
                    "term" : {
                        "uri" : "lookup/Testing/68d12f2d-283b-4858-be0f-9278a6354caf"
                    }
                }
            }
        }
        }
        **/
        JSONObject uriObject = new JSONObject();
        uriObject.put("uri",message.getUri());

        JSONObject termObject = new JSONObject();
        termObject.put("term", uriObject);


        JSONObject filterObject = new JSONObject();
        filterObject.put("filter",termObject);

        JSONObject csObject = new JSONObject();
        csObject.put("constant_score",filterObject);


        JSONObject queryObject = new JSONObject();
        queryObject.put("query",csObject);
        boolean waitFlag = true;
        boolean errorStatus= false;

        //Send the query
        String postUrl = LatencyChecker.SLSCACHEENDPOINT ;
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(postUrl);
        post.setHeader("Content-type", "application/json");
        try
        {
            StringEntity stringEntity = new StringEntity(queryObject.toString());
            post.setEntity(stringEntity);
        }
        catch(UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }



        CloseableHttpResponse response = null;

        while (true)
        {

            try
            {
                executionCount++;
                response = httpClient.execute(post);

                if (response.getStatusLine().getStatusCode() == 200)
                {

                    String json = EntityUtils.toString(response.getEntity());
                    JSONObject jsonObject = JSONObject.fromObject(json);
                    JSONObject hitsMap = (JSONObject) jsonObject.get("hits");
                    Integer total = (Integer) hitsMap.get("total");

                    if (total.intValue() > 0)
                    {

                        JSONArray hits = (JSONArray) hitsMap.get("hits");
                        if (hits.size() != 0)
                        {
                            JSONObject hits2 = (JSONObject) hits.get(0);
                            JSONObject sourceHashMap = (JSONObject) hits2.get("_source");

                            String recvURI = (String) sourceHashMap.get("uri");
                            Date creationTime = (Date) JSONObject.toBean((JSONObject) sourceHashMap.get("createdInCache")
                                    , Date.class);

                            TimeZone tz = TimeZone.getTimeZone("UTC");
                            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                            dateFormat.setTimeZone(tz);
                            Date recvExpiryDate = dateFormat.parse((String) sourceHashMap.get("expires"));
                            Object expiryString = sourceHashMap.get("expires");

                            if (recvURI.equals(uri))
                            {

                                if (message.getMessageType().equals(LGMessage.REGISTER))
                                {
                                    saveData(expiryString, creationTime, LGMessage.REGISTER);
                                    if (message.getIsStored() == true)
                                    {
                                        LatencyChecker.storeInfo(recvURI, (String) sourceHashMap.get("expires"));
                                    }
                                    System.out.println("message:" + message.getMessageId() + " Finished" + "--"
                                       + message.getMessageType() + " uri:" + message.getUri() + " errors?:"+ errorStatus);

                                    waitFlag = false;

                                }
                                else if (message.getMessageType().equals(LGMessage.RENEW))
                                {

                                    Date storedExpiryDate = LatencyChecker.uriExpiryMap.get(message.getUri());
                                    if (storedExpiryDate != null)
                                    {
                                        if (storedExpiryDate.compareTo(recvExpiryDate) > 0)
                                        {
                                            LatencyChecker.uriExpiryMap.put(message.getUri(), message.getExpiresDate());
                                            saveData(expiryString, creationTime, LGMessage.RENEW);
                                            System.out.println("message:" + message.getMessageId() + " Finished" + "--"
                                                    + message.getMessageType() + " uri:" + message.getUri() + " errors?:"+ errorStatus);
                                            waitFlag = false;
                                        }
                                        else
                                        {
                                       /* System.out.println("Stored:"+ storedExpiryDate.toString() + " Recev:"
                                                + message.getExpiresDate().toString() + "uri:" + message.getUri());*/
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                else
                {
                    System.out.println("ERROR:::  Status Code from Cache: " + response.getStatusLine().getStatusCode()
                                                + " uri: " + message.getUri());
                    errorStatus = true;

                    try
                    {
                        Thread.sleep(60000);
                    }
                    catch (Exception e)
                    {

                    }
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            finally
            {
                try
                {
                    response.close();
                }
                catch(Exception e)
                {}

            }

            //Sleep:
            try
            {
                if(executionCount < EXECUTIONLIMIT && waitFlag)
                {
                   // System.out.println("message:" + message.getMessageId() + " SLEEP " + "--"
                     //       + message.getMessageType() + "uri:" + message.getUri());

                    Thread.sleep(REGISTERLOOPTIME);
                }
                else
                {
                    if(waitFlag!=false)
                    {
                        System.out.println("message:" + message.getMessageId() + " TimedOut " + "--"
                                + message.getMessageType() + " uri:" + message.getUri() + " errors?:"+ errorStatus );
                        saveData(null, null, message.getMessageType());
                    }

                    //clean up
                    post.releaseConnection();
                    httpClient.close();
                    return;// exit this thread. we are done.
                }
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * Saves the date in the final elasticsearch instance
     * @param expiry
     * @param creationTimeStamp
     * @param messageType
     */
    public void saveData(Object expiry, Date creationTimeStamp, String messageType)
    {
        //get all required parameters.
        HashMap<String,Object> resultMap = new HashMap<String,Object>();

        //M
        resultMap.put("M",LatencyChecker.M);
        //N
        resultMap.put("N",LatencyChecker.N);
        //T
        resultMap.put("T",LatencyChecker.T);

        //Response Time
        resultMap.put("latency",calculateTimeDifference(creationTimeStamp));

        //uri
        resultMap.put("uri",message.getUri());

        //messageType
        resultMap.put("messageType",messageType);

        //expires TimeStamp
        resultMap.put("expires",expiry);

        //creationTimeStamp
        DateTime dt = new DateTime();
        DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
        String str = fmt.print(dt);


        resultMap.put("creationTime:" , str);

        Gson gson = new Gson();
        String json = gson.toJson(resultMap, HashMap.class);

        //send to ES store
        try
        {

            CloseableHttpClient httpClient    = HttpClients.createDefault();
            HttpPost     post          = new HttpPost(LatencyChecker.DATASTOREENDPOINT);
            post.setHeader("Content-type", "application/json");
            StringEntity stringEntity = new StringEntity(json);
            post.setEntity(stringEntity);

            CloseableHttpResponse response = httpClient.execute(post);

            // get back response.
            if(response.getStatusLine().getStatusCode() == 201)
            {
                //System.out.println("Response stored!");
            }
            else
            {
                System.err.println("Status response from Data Store: " + response.getStatusLine().getStatusCode());

            }

            //clean up
            response.close();
            post.releaseConnection();
            httpClient.close();

        }
        catch(IOException e)
        {
            System.out.println("There's an error in the sending the http renew request");

        }
    }

    /**
     * Calculates the latency
     */
    public long calculateTimeDifference(Date creationTimeStamp)
    {
        //calculate TimeDifference:
        if(creationTimeStamp==null)
        {
            return DROP;
        }
        Date successTime = message.getTimestamp();


        if(creationTimeStamp.getTime()-successTime.getTime() < 0)
        {
            System.out.println("successTime:"+successTime.getTime()+" cTime:"+creationTimeStamp.getTime());
        }
        return  creationTimeStamp.getTime() - successTime.getTime() ;
    }

}
