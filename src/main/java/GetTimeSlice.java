import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.tools.ant.taskdefs.Local;

import java.sql.Date;
import java.time.Duration;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class GetTimeSlice extends UDF {
    private final static double interval = 10800F;

    LocalTime s1 = LocalTime.MIDNIGHT;
    LocalTime s2 = LocalTime.parse("03:00:00");
    LocalTime s3 = LocalTime.parse("06:00:00");
    LocalTime s4 = LocalTime.parse("09:00:00");
    LocalTime s5 = LocalTime.parse("12:00:00");
    LocalTime s6 = LocalTime.parse("15:00:00");
    LocalTime s7 = LocalTime.parse("18:00:00");
    LocalTime s8 = LocalTime.parse("21:00:00");
    LocalTime s9 = LocalTime.parse("23:59:59");

    private LocalTime[] timeState = new LocalTime[]{s1,s2,s3,s4,s5,s6,s7,s8,s9};



    public String[] evaluate(String s, String e) {
        if(e.equals("00:00:00")){
            e = "23:59:59";
        }
        LocalTime start=LocalTime.parse(s);
        LocalTime end=LocalTime.parse(e);
        String[] times = new String[8];
        for(Integer i = 0; i < timeState.length - 1; i++){
            // 起始时间落在哪个时间段内
            if((start.isAfter(timeState[i]) || start.equals(timeState[i])) && start.isBefore(timeState[i+1])){
                // 跨越时间段
                if(end.isAfter(timeState[i+1]) || end.equals(timeState[i+1])){
                    // 本时间段时间
                    times[i] = String.valueOf(Duration.between(start,timeState[i+1]).getSeconds());
                    start = timeState[i+1];
                }
                else{
                    times[i] = String.valueOf(Duration.between(start,end).getSeconds());
                }
            }
            else{
                times[i] = String.valueOf(0);
            }
        }
        return times;
    }

    public static void main(String[] args) {
        String s = "12:18:11", e = "00:00:00";

        GetTimeSlice g = new GetTimeSlice();
        String[] strings = g.evaluate(s,e);
        LongWritable[] longs = new LongWritable[strings.length];
        for (int i = 0; i < longs.length; i++) {
            System.out.println(Long.valueOf(strings[i]));
        }
    }
}
