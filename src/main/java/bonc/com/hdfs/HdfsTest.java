package bonc.com.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.registry.client.impl.zk.RegistryInternalConstants.HADOOP_USER_NAME;

public class HdfsTest {
	private static final Log LOG = LogFactory.getLog(HdfsTest.class.getName());

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub\

		System.setProperty("HADOOP_USER_NAME","hadoop");
		String path = "/user/bonc_fjl";
		//HDFSTool.listDir(path);
		//HDFSTool.readFile(path);
		//HDFSTool.reOwner(path,"bonc_fjl","hadoop");
        //HDFSTool.testPut("/opt/beh/core/hadoop/etc/hadoop/capacity-scheduler.xml","/tmp");
	}
}
