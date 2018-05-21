package bonc.com.shell;

/**
 * @ProjectName: javademo
 * @Package: bonc.com.shell
 * @ClassName: Basic
 * @Description: java类作用描述
 * @Author: Chendeyong
 * @CreateDate: 2018/5/15 10:09
 * @Version: 1.0
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class Basic
{
    public static void main(String[] args)
    {
        String hostname = "172.16.3.45";
        String username = "bonc_gjj";
        String password = "bonc123";
        System.setProperty("user.name","hadoop");

        try
        {
            Connection conn = new Connection(hostname);
            conn.connect();

            boolean isAuthenticated = conn.authenticateWithPassword(username, password);

            if (isAuthenticated == false)
                throw new IOException("Authentication failed.");

            Session sess = conn.openSession();
            sess.execCommand("ls /opt/beh");
            System.out.println("Here is some information about the remote host:");

            InputStream stdout = new StreamGobbler(sess.getStdout());
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout));

            while (true)
            {
                String line = br.readLine();
                if (line == null)
                    break;
                System.out.println(line);
            }
            System.out.println("ExitCode: " + sess.getExitStatus());
            sess.close();
            conn.close();
        }
        catch (IOException e)
        {
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }
}