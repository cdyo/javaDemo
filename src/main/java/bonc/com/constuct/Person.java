package bonc.com.constuct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;

//可变对象的返回器是不可取的

public class Person {
    private static final Log LOG = LogFactory.getLog(Person.class.getName());

    public static int nextid = 1;
    private Date hireDay;
    private int id;

    public void setId(){
        id=nextid;
        nextid++;
    }

    public int getId(){
        return id;
    }

    /** 
    * @Description:
    * @Param: [hireDay] 
    * @return: void 
    */ 
    public void setHireDay(Date hireDay){
        this.hireDay=hireDay;
    }

    public Date getHireDay() {
       // return hireDay;
        return (Date) hireDay.clone();
    }

    public static void main(String[] args) {
        Person person = new Person();
        person.setId();
        //person.setId();
        long time = System.currentTimeMillis();
        person.setHireDay(new Date(time));

        Date date = person.getHireDay();
        LOG.info("date "+date);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        date.setTime(System.currentTimeMillis());
        LOG.info("date2"+person.getHireDay());
        LOG.info(person.getId());
    }
}


