package bonc.com.kafka.consumer.avro;

import bonc.com.util.LoadPropertiesFile;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.util.Properties;

/** 
* AvroKafkaConsumerNew Tester. 
* 
* @author <Authors name> 
* @since <pre>���� 26, 2018</pre> 
* @version 1.0 
*/ 
public class AvroKafkaConsumerNewTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: testConsumer() 
* 
*/ 
@Test
public void testTestConsumer() throws Exception { 
//TODO: Test goes here...
    Properties prop = LoadPropertiesFile.initProperties("src/main/resources/kafka.properties");
    String topic = prop.getProperty("topic");
    String hosts=prop.getProperty("bootstrap.servers");

    String groupid = "cdytest";
    AvroKafkaConsumerNew consumer = new AvroKafkaConsumerNew(hosts,"test1",topic);
    consumer.testConsumer();
} 


} 
