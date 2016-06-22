/**
 * Runner:
 * Main class for running the feeder and KeyValueGenerator Threads.
 */
import java.util.Calendar;
import java.util.concurrent.Semaphore;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Runner
{


    public static Semaphore feederSemaphore = new Semaphore(1);
    public static Semaphore kvgSemaphore = new Semaphore(0);

    public static long currentRenewRequests = 0;
    public static long currentRegisterRequests = 0;
    public static final int POLLINGPERIOD = 60* 1000; // 1 minute

    public static final int LOG = 1;
    public static final int DIST = 2;
    public static int mode = -1;


    /**
     * Print Usage
     */

    public static void printUsage()
    {
        System.out.println("Usage: java Feeder <RequestGenerationMode> (Distribution)");
        System.out.println("****Request Generation Mode Options***");
        System.out.println("1 --- Log based");
        System.out.println("2 --- Distribution based");

        System.out.println("****Distribution Options***");
        System.out.println("PO -- Poisson");
        System.out.println("HT -- Heavy-Tailed");
    }

    /**
     * Get random time within the last 24 hours
     */
    public static Date getSeedTime()
    {
        //get current timestamp;

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Calendar calendar = Calendar.getInstance();
        System.out.println(dateFormat.format(calendar.getTime())); //2014/08/06 16:00:22

        //go back 24 hours
        calendar.add(Calendar.HOUR_OF_DAY,-24*10);
        //2016-06-11T14:43:35

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        System.out.println(df.format(calendar.getTime()));
        return calendar.getTime();
    }

    public static void main(String[] args) throws Exception
    {


        System.out.println("Feeder Starting..");

        /*Log Feeder*/
        Feeder feeder = new Feeder();
        feeder.setSeedTimeStamp(getSeedTime());
        feeder.setPeriod(POLLINGPERIOD);
        Thread feederThread = new Thread(feeder);
        feederThread.start();

        /*Key Value Generator*/
        KeyValueGenerator kvGenerator = new KeyValueGenerator();
        Thread keyValueGenThread = new Thread(kvGenerator);
        keyValueGenThread.start();

    }
}
