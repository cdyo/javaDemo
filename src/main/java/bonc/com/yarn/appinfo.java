package bonc.com.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

/**
 * @ProjectName: javademo
 * @Package: bonc.com.yarn
 * @ClassName: appInfo
 * @Description: 获取yarn Applications信息
 * @Author: Chendeyong
 * @CreateDate: 2018/5/11 14:15
 * @Version: 1.0
 */

public class appinfo {
    private static final Log LOG = LogFactory.getLog(appinfo.class.getName());
    private static Configuration conf;
    static {
        conf = new Configuration();
        conf.addResource("src/main/resources/yarn-site.xml");
    }
    private static YarnClient client = YarnClient.createYarnClient();

    /**
    * @Description: 获取yarn Application详细信息
     * @param :null
    * @return:
    */
    public static void listAppstatus() {
        client.init(conf);
        client.start();
        EnumSet<YarnApplicationState> applicationStates = EnumSet.noneOf(YarnApplicationState.class);
        if (applicationStates.isEmpty()) {
            applicationStates.add(YarnApplicationState.RUNNING);
            applicationStates.add(YarnApplicationState.ACCEPTED);
            applicationStates.add(YarnApplicationState.SUBMITTED);
        }
        List<ApplicationReport> applicationReports = null;
        try {
            applicationReports = client.getApplications(applicationStates);
        } catch (YarnException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }

        for (ApplicationReport appReport : applicationReports) {
            ApplicationReportPBImpl app = (ApplicationReportPBImpl) appReport;
            String appId = app.getApplicationId().toString();
            String appName = app.getName();
            String appType = app.getApplicationType();
            String appUser = app.getUser();
            String appQueue = app.getQueue();
            String appState = app.getYarnApplicationState().toString();
            String appStartTime = timeStamp2Date(app.getStartTime());
            LOG.info("AppID:"+appId);
            LOG.info("appType:"+appType);
            LOG.info("appUser:"+appUser);
            LOG.info("appQueue:"+appQueue);
            LOG.info("appState:"+appState);
            LOG.info("appStartTime:"+appStartTime);
            LOG.info("-----------------------------------------------");
        }


    }

    /**
    * @Description: 时间戳转换 "yyyy-MM-dd HH:mm:ss"时间格式
     * @param :Long 时间戳
    * @return:
    */
    public static String timeStamp2Date(Long timeStamp) {
        if (timeStamp == null) {
            return "";
        }
         String format = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(timeStamp));
    }
}
