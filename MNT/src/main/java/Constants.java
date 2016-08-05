import java.util.HashMap;

/**
 * Created by kamala on 8/4/16.
 */
public class Constants {
    public static String LOGHOSTNAME = "";
    public static final String configFile = "logfeeder.yaml";

    public static void initializeConstants()
    {
        HashMap<String,Object> constantMap = (HashMap<String, Object>) BaseConfigReader.getInstance().getConfiguration(configFile);

        LOGHOSTNAME = (String) constantMap.get("log_host_name");
    }
}
