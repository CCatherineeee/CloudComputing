package MapReduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.Map;

class LongArrayWritable extends ArrayWritable {

    public LongArrayWritable() {
        super(LongWritable.class);
    }

    public LongArrayWritable(String[] string) {
        super(LongWritable.class);
        LongWritable[] longs = new LongWritable[string.length];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = new LongWritable(Long.valueOf(string[i]));
        }
        set(longs);
    }


}