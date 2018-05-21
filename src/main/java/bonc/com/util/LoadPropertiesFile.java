package bonc.com.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.Properties;

/**
 * Load Properties File
 * @author zhuxq
 */
public class LoadPropertiesFile {
    private static final Log LOG = LogFactory.getLog(LoadPropertiesFile.class.getName());
    private static Properties prop = new Properties();
    public static Properties initProperties(String basePath) {
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(
                    new File(basePath)));
            prop.load(in);

        } catch (FileNotFoundException e) {
            LOG.error("properties path wrong!，please check！");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error(e.getMessage());
        }
        return prop;
    }

}