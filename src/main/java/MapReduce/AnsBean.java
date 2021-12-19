package MapReduce;


import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;

public class AnsBean implements Writable {

    private String usr;


    public AnsBean(){
        super();
    }

    public AnsBean(String s,String e){
        LocalTime s1 = LocalTime.MIDNIGHT;
        LocalTime s2 = LocalTime.parse("03:00:00");
        LocalTime s3 = LocalTime.parse("06:00:00");
        LocalTime s4 = LocalTime.parse("09:00:00");
        LocalTime s5 = LocalTime.parse("12:00:00");
        LocalTime s6 = LocalTime.parse("15:00:00");
        LocalTime s7 = LocalTime.parse("18:00:00");
        LocalTime s8 = LocalTime.parse("21:00:00");
        LocalTime s9 = LocalTime.parse("23:59:59");

        LocalTime[] timeState = new LocalTime[]{s1,s2,s3,s4,s5,s6,s7,s8,s9};

        LocalTime start=LocalTime.parse(s);
        LocalTime end=LocalTime.parse(e);
        Long[] times = new Long[8];
        for(Integer i = 0; i < timeState.length - 1; i++){
            // 起始时间落在哪个时间段内
            if((start.isAfter(timeState[i]) || start.equals(timeState[i])) && start.isBefore(timeState[i+1])){
                // 跨越时间段
                if(end.isAfter(timeState[i+1]) || end.equals(timeState[i+1])){
                    // 本时间段时间
                    times[i] = Duration.between(start,timeState[i+1]).getSeconds();
                    start = timeState[i+1];
                }
                else{
                    times[i] = Duration.between(start,end).getSeconds();
                    break;
                }
            }
        }

    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}