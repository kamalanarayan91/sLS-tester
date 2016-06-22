import  java.util.*;
import java.io.*;


import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;

/**
 * Created by kamala on 6/6/16.
 */
public class PoissonDistributionFeeder extends RMQEndPoint implements Runnable
{
    private double mean;
    private long lastExecutionTime = -1;

    private long interval=1100; // 1 second
    private AbstractIntegerDistribution distribution;
    private double ratio = 0.9; //90% register and 10% renew
    private static final long requestTime = 10; // ms
    private static final String RENEW = "RENEW";
    private static final String REGISTER = "REGISTER";
    private static final String QUEUENAME = "Q2";
    private static final int FACTOR = 1000;


    public PoissonDistributionFeeder(double mean)
    {
        super(QUEUENAME);
        this.mean = mean;
        distribution = new PoissonDistribution(mean);
    }


    public long getRandomNumber()
    {
        return distribution.sample();
    }

    public void run()
    {
        while(true)
        {
            /*Sleep if one second hasn't passed */
            long sleepTime = getRandomNumber() * FACTOR;
            long difference = sleepTime - requestTime;

            if(difference < 0)
            {
                continue;
            }

            try
            {
                Thread.sleep(difference);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }

            String requestType = getRequestType();
            String uri = sendRequest(requestType);

        }
    }

    public String getRequestType()
    {
        return null;
    }

    public String sendRequest(String s)
    {
        return null;
    }

}
