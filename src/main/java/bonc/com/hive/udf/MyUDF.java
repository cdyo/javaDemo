package bonc.com.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "hello", value = "_FUNC_(x) - return a string concat hello",
        extended = "Example: hello(\"me\") ")

public class MyUDF extends UDF{
    public String evaluate(String str){
        if ( str != null) {
            return "hello"+str;
        } else {
            return null;
        }
    }
}
