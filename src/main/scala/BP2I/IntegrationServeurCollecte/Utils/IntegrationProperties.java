package BP2I.IntegrationServeurCollecte.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class IntegrationProperties {

    public static String tabParamDir;
    static String appParamDir;
    static String goodDir;
    static String badDir;
    static String hdfsDir;

    public static void setPropValues() throws IOException {

        Properties prop = new Properties();

        String propFileName = Environment.env.toLowerCase() + ".properties";

        if (!IntegrationParams.environments.contains(propFileName)) {

            System.out.println("THIS ENVIRONMENT DOES NOT EXIST: " + propFileName);
            System.exit(0);
        }

        InputStream inputStream = IntegrationProperties.class.getClassLoader().getResourceAsStream(propFileName);

        prop.load(inputStream);

        appParamDir = prop.getProperty("param.int.appParamDir");
        tabParamDir = prop.getProperty("param.int.tabParamDir");
        goodDir = prop.getProperty("param.int.goodDir");
        badDir = prop.getProperty("param.int.badDir");
        hdfsDir = prop.getProperty("param.int.hdfsDir");
    }
}