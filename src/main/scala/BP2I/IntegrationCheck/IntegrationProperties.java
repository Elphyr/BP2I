package BP2I.IntegrationCheck;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

class IntegrationProperties {

    private InputStream inputStream;
    private String propValue = "";

    IntegrationProperties() throws IOException {
    }

    private String getPropValue(String propName) throws IOException {

        try {
            Properties prop = new Properties();
            String propFileName = Environment.env.toLowerCase() + ".properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);

            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath.");
            }

            propValue = prop.getProperty("param." + propName);

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            Objects.requireNonNull(inputStream).close();
        }
        return propValue;
    }

    String appParamDir  = getPropValue("appParamDir");
    String tabParamDir  = getPropValue("tabParamDir");
    String goodDir      = getPropValue("goodDir");
    String badDir       = getPropValue("badDir");
    String hdfsDir      = getPropValue("hdfsDir");
}