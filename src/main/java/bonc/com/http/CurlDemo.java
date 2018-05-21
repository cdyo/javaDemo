package bonc.com.http;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

public class CurlDemo {
    private static final Log LOG = LogFactory.getLog(CurlDemo.class.getName());

    public static void main(String args[])  {
        URL url = null;
        try {
            url = new URL("https://www.baidu.com");
        } catch (MalformedURLException e) {
            LOG.error(e.getMessage());
        }
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
            for (String line; (line = reader.readLine()) != null; ) {
                System.out.println(line);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }finally {
            try {
                reader.close();
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }

    }
}