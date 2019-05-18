import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtiles {
    @Test
    public void getDayOfNowMin(){

        Calendar cal = Calendar.getInstance();
//        System.out.println(cal.getTimeInMillis());
//        Date time = cal.getTime();
//        String s = new SimpleDateFormat("yyyy年MM月dd日").format(new Date());
//        System.out.println(s);
        cal.set(Calendar.HOUR_OF_DAY,0);
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);
        cal.set(Calendar.MILLISECOND,0);
//        Date time = cal.getTime();
        System.out.println(cal.getTime());
         long todayMinTime = cal.getTimeInMillis();
        System.out.println(todayMinTime);//获取当日的零点时间
        //获取昨日零点的时间戳
        cal.add(Calendar.DATE,-1);
        long yestoday = cal.getTimeInMillis();
        System.out.println("获取昨日零点的时间戳:"+"\n\t"+ yestoday);


    }

}
