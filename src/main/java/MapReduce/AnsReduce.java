package MapReduce;



import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import org.stringtemplate.v4.ST;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AnsReduce extends Reducer<Text, LongArrayWritable, Text, NullWritable>{

    private Text key3 = new Text();


    @Override
    protected void reduce(Text key,Iterable<LongArrayWritable> val2s,Context context) throws IOException, InterruptedException{
        Long sum = Long.valueOf(0);
        Long l1 =  Long.valueOf(0);
        Long l2 =  Long.valueOf(0);
        Long l3 =  Long.valueOf(0);
        Long l4 =  Long.valueOf(0);
        Long l5 =  Long.valueOf(0);
        Long l6 =  Long.valueOf(0);
        Long l7 =  Long.valueOf(0);
        Long l8 =  Long.valueOf(0);

        for(LongArrayWritable l : val2s){
            Writable[] writables = l.get();
            l1 += Long.valueOf(writables[0].toString());
            l2 += Long.valueOf(writables[1].toString());
            l3 += Long.valueOf(writables[2].toString());
            l4 += Long.valueOf(writables[3].toString());
            l5 += Long.valueOf(writables[4].toString());
            l6 += Long.valueOf(writables[5].toString());
            l7 += Long.valueOf(writables[6].toString());
            l8 += Long.valueOf(writables[7].toString());
        }

        sum += l1+l2+l3+l4+l5+l6+l7+l8;
        double d1 = (double) l1/(double) sum;
        double d2 = (double) l2/(double) sum;
        double d3 = (double) l3/(double) sum;
        double d4 = (double) l4/(double) sum;
        double d5 = (double) l5/(double) sum;
        double d6 = (double) l6/(double) sum;
        double d7 = (double) l7/(double) sum;
        double d8 = (double) l8/(double) sum;

        key3.set(key + "\t" + d1 + "\t" + d2 + "\t" + d3 + "\t" + d4 + "\t" + d5 + "\t" + d6 +"\t" + d7 + "\t" + d8);
        context.write(key3, NullWritable.get());
    }
}
