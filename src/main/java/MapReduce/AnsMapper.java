package MapReduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.stringtemplate.v4.ST;


import java.awt.geom.FlatteningPathIterator;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class AnsMapper extends Mapper<LongWritable, Text,Text, LongArrayWritable> {


    Text k = new Text();

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
                    for(Integer j = i+1; j< timeState.length - 1; j++){
                        times[j] = String.valueOf(0);
                    }
                }
            }
            else{
                times[i] = String.valueOf(0);
            }
        }
        return times;


    }

    @Override
    public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
        String line = text.toString();

        System.out.println("text读出的数据: "+line);
        String[] fields = line.split("\t");

        String caller = fields[1];

        String s = fields[9];
        String e = fields[10];

        k.set(caller);
        String[] times = evaluate(s,e);

        LongArrayWritable arrayWritable = new LongArrayWritable(times);
        context.write(k, arrayWritable);

    }



}
