package bonc.com.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

class HDFSTool {
    private static final Log LOG = LogFactory.getLog(HDFSTool.class.getName());
    private static Configuration conf;

    static {
        conf = new Configuration();
        conf.addResource("src/main/resources/hdfs-site.xml");
    }

    /**
     * @param :文件目录
     * @Description: cat 文件
     * @return: void
     */
    public static void readFile(String args0) throws IOException {
        FileSystem hdfs = hdfs = FileSystem.get(conf);
        Path path = new Path(args0);
        if (hdfs.getFileStatus(path).isFile()) {
            FSDataInputStream is = hdfs.open(path);
            IOUtils.copyBytes(is, System.out, 4096, false);
            IOUtils.closeStream(is);
        }else {
            LOG.error(args0+":is not a file");
        }
    }


    /**
     * @param :修改权限
     * @param args
     * @Description: cat 文件
     * @return: void
     */
    public static void reOwner(String args,String owner,String group )throws IOException {
        FileSystem hdfs = hdfs = FileSystem.get(conf);
        Path path = new Path(args);
        //FileSystem hdfs = path.getFileSystem(conf);
        FileStatus status = hdfs.getFileStatus(path);
        LOG.info(args+" 原始权限: "+status.getOwner()+":"+status.getGroup()+" "+status.getPermission());
        hdfs.setPermission(path,new FsPermission(FsAction.ALL,FsAction.NONE,FsAction.NONE));
        hdfs.setOwner(path,owner,group);
        FileStatus status2 = hdfs.getFileStatus(path);
        LOG.info(args+" 修改后权限: "+status2.getOwner()+":"+status2.getGroup()+" "+status2.getPermission());
    }

    /**
    * @Description: 列出目录
     * @param :args1 文件路径
     * @return:
    */
    public static void listDir(String args) throws FileNotFoundException, IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args);
        FileStatus[] fs = hdfs.listStatus(path);
        for (FileStatus f : fs) {
            Path p = f.getPath();
            String info = f.isDir() ? "目录" : "文件";
            LOG.info(info + ":" + p.toString());
        }
    }

    /**
    * @Description:创建目录 类似mkdir -p
     * @param:路径
    * @return:
    */
    public static void mkDir(String args2) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args2);
        boolean b = hdfs.mkdirs(path);
        String s = b ? "success" : "fail";
        LOG.info(args2 + " create " + s);
    }

    /**
    * @Description:上传文件 Hadoop fs -put
     * @param :本地路径，hdfs路径
    * @return:
    */
    public static void testPut(String args3, String args4) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path srcpath = new Path(args3);
        Path hdfspath = new Path(args4);
        hdfs.copyFromLocalFile(srcpath, hdfspath);
        LOG.info("put file finish!");
    }

    /**
    * @Description:从console里写入hdfs
     * @param:hdfs 路径
    * @return:
    */
    public static void testCreate(String args5) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args5);
        Scanner scanner;
        try (FSDataOutputStream fo = hdfs.create(path)) {
            scanner = new Scanner(System.in);
            LOG.info("put exit finish programe");
            while (true) {
                String line = scanner.nextLine();
                if (line.equals("exit")) {
                    break;
                }
                fo.writeChars(line);
                fo.writeChars("\n");
            }
            fo.close();
        }
        scanner.close();
    }

    /**
    * @Description:更名
     * @param:null
    * @return:
    */
    public static void testRename(String args1, String args2) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path srcpath = new Path(args1);
        Path destpath = new Path(args2);
        if (hdfs.isFile(srcpath)) {
            boolean b = hdfs.rename(srcpath, destpath);
            if (b) {
                LOG.info("文件更名成功");
            } else {
                LOG.error("文件更名失败");
            }
        } else {
            LOG.error("所输入路径"+args1+"不是文件");
        }
    }

    /**
    * @Description:删除文件
     * @param:null
    * @return:
    */
    public static void deleteFile(String args) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args);
        boolean b = hdfs.deleteOnExit(path);
        if (b) {
            LOG.info("文件删除成功");
        } else {
            LOG.info("文件删除失败");
        }

    }

    /**
    * @Description:返回datanode hostname
     * @param:
    * @return:
    */
    public static void getDatanode() throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        DistributedFileSystem dfs = (DistributedFileSystem) hdfs;
        DatanodeInfo[] df = dfs.getDataNodeStats();
        for (DatanodeInfo datanodeinfo : df) {
            String host = datanodeinfo.getHostName();
            LOG.info("DataNode:"+host);
        }
    }

    /**
    * @Description:返回文件信息
     * @param:null
    * @return:
    */
    public static void getFileinfo(String args) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path(args);
        if (hdfs.isFile(path)) {
            long size = hdfs.getBlockSize(path);
            short st = hdfs.getReplication(path);
            FileStatus fs = hdfs.getFileStatus(path);
            long L = fs.getLen();
            LOG.info("文件"+args+":");
            LOG.info("副本数:"+st);
            LOG.info("块大小:"+size/1024/1024+"MB");
            LOG.info("文件大小:"+L/1024/1024+"MB");

            BlockLocation[] bl = hdfs.getFileBlockLocations(path, 0, L);
            int i = 1;
            for (BlockLocation b : bl) {
                LOG.info("第"+i+"块");
                String[] hosts = b.getHosts();
                for (String host : hosts) {
                    LOG.info("主机名:"+host);
                }
                i++;
            }
        }
    }

    /**
    * @Description:合并上传本地文件到hdfs
     * @param:null
    * @return:
    */
    public static void putMerge(String args1, String args2) throws IOException {
        Path locdir = new Path(args1);//�����ļ�Ŀ¼
        Path hdfspath = new Path(args2);
        try {
            FileSystem hdfs = FileSystem.get(conf);
            FileSystem locfs = FileSystem.getLocal(conf);
            FileStatus[] fileStatus = locfs.listStatus(locdir);
            try (FSDataOutputStream hdfsos = hdfs.create(hdfspath)) {
                for (FileStatus fs : fileStatus) {
                    if (fs.isFile()) {
                        try (FSDataInputStream fin = locfs.open(fs.getPath())) {
                            byte[] buffer = new byte[1024];
                            int len = 0;
                            while ((len = fin.read(buffer)) > 0) {
                                hdfsos.write(buffer, 0, len);

                            }
                            fin.close();
                        }
                    }
                }
                hdfsos.close();
            }
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }
        LOG.info("finished!");
    }
}
