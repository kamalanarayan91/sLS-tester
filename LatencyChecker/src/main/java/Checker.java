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

    /** Configure **/
    public static final int SLEEPINTERVAL = 30000;//30 seconds
    public static final int EXECUTIONLIMIT = 60; //30 mins total
    public static final long INITIALSLEEPTIME = 10 * 1000; // 10 seconds
    /** End - Configure **/

    private int executionCount;
    public static final String REGISTEREDINCACHE = "registered";
    public static final String RENEWEDINCACHE = "renewed";

    public Checker(LGMessage message)
    {
        executionCount= 0;
        this.message = message;
        Log log = LogFactory.getLog(JSONObject.class);
    }

    public void run()
    {

        try
        {
            Thread.sleep(INITIALSLEEPTIME);
        }
        catch(Exception e)
        {

        }



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




        CloseableHttpResponse response = null;

        while (true)
        {

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
                            int index = 0;
                            while(index<hits.size())
                            {
                                JSONObject hits2 = (JSONObject) hits.get(index);
                                index++;


                                JSONObject sourceHashMap = (JSONObject) hits2.get("_source");

                                String recvURI = (String) sourceHashMap.get("uri");
                                Date creationTime = (Date) JSONObject.toBean((JSONObject) sourceHashMap.get("createdInCache")
                                        , Date.class);

                                TimeZone tz = TimeZone.getTimeZone("UTC");
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                                dateFormat.setTimeZone(tz);
                                Date recvExpiryDate = dateFormat.parse((String) sourceHashMap.get("expires"));
                                Object expiryString = sourceHashMap.get("expires");
                                String state = (String) sourceHashMap.get("state");

                                if (message.getMessageType().equals(LGMessage.REGISTER))
                                {
                                    if(state.equals(REGISTEREDINCACHE) == false)
                                    {
                                        continue;
                                    }
                                    saveData(expiryString, creationTime, LGMessage.REGISTER);

                                    if (message.getIsStored() == true)
                                    {
                                        LatencyChecker.storeInfo(recvURI, (String) sourceHashMap.get("expires"));
                                    }

                                    System.out.println("message:" + message.getMessageId() + " Finished" + "--"
                                            + message.getMessageType() + " uri:" + message.getUri());

                                    waitFlag = false;
                                    break;

                                }
                                else if (message.getMessageType().equals(LGMessage.RENEW))
                                {
                                    if(state.equals(RENEWEDINCACHE) == false)
                                    {
                                        continue;
                                    }

                                    Date storedExpiryDate = LatencyChecker.uriExpiryMap.get(message.getUri());
                                    if (storedExpiryDate != null)
                                    {

                                        if(storedExpiryDate.before(recvExpiryDate))
                                        {
                                            LatencyChecker.uriExpiryMap.put(message.getUri(), message.getExpiresDate());

                                            saveData(expiryString, creationTime, LGMessage.RENEW);

                                            System.out.println("message:" + message.getMessageId() + " Finished" + "--"
                                                    + message.getMessageType() + " uri:" + message.getUri());

                                            waitFlag = false;
                                            break;
                                        }
                                        else
                                        {
                                           // System.out.println(" State:"+ state + " Stored:" + storedExpiryDate + " Received:" + recvExpiryDate +" json:"+ json);
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                       // System.out.println("Renew of something without register"+" uri:" + message.getUri());
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
                    //clean up
                    response.close();
                    httpClient.close();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }

            }

            //Sleep:
            try
            {
                if(executionCount < EXECUTIONLIMIT && waitFlag)
                {
                   // System.out.println("message:" + message.getMessageId() + " SLEEP " + "--"
                     //       + message.getMessageType() + "uri:" + message.getUri());

                    Thread.sleep(SLEEPINTERVAL);
                }
                else
                {
                    if(waitFlag!=false)
                    {
                        System.out.println("message:" + message.getMessageId() + " TimedOut " + "--"
                                + message.getMessageType() + " uri:" + message.getUri() + " errors?:"+ errorStatus );

                        saveData(null, null, message.getMessageType());
                    }


                    return;// exit this thread. we are done.
                }
            }
            catch (InterruptedException e)
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

        long time = calculateTimeDifference(creationTimeStamp);
        if(time > EXECUTIONLIMIT* SLEEPINTERVAL)
        {
            time = DROP;
        }

        if(time != DROP)
        {
            //Response Time
            resultMap.put("latency", time);
            resultMap.put("result","SUCCESS");
        }
        else
        {
            resultMap.put("result","TIMEOUT");
        }

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
    public long calculateTimeDifference(Date cacheCreationTimeStamp)
    {
        //calculate TimeDifference:
        if(cacheCreationTimeStamp==null)
        {
            return DROP;
        }
        Date coreSuccessTime = message.getTimestamp();


        if(cacheCreationTimeStamp.getTime()-coreSuccessTime.getTime() < 0) // impossible
        {
            System.out.println("successTime:"+coreSuccessTime.getTime()+" cTime:"+cacheCreationTimeStamp.getTime());
        }
        return  cacheCreationTimeStamp.getTime() - coreSuccessTime.getTime() ;
    }

}

