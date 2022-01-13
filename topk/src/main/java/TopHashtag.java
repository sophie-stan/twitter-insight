import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopHashtag {

    private static final int K = 10;

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        private final TreeMap<Integer, Text> TopKMap = new TreeMap<>();

        public void map(LongWritable key, Text value, Context context) {

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer k : TopKMap.keySet()) {
                context.write(new Text(k.toString()), TopKMap.get(k));
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
        private static final TreeMap<Text, Text> TopKMap = new TreeMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) {

            for (Text value : values) {
                TopKMap.put(new Text(key), new Text(value));
                if (TopKMap.size() > K) {
                    TopKMap.remove(TopKMap.firstKey());
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text k : TopKMap.keySet()) {
                context.write(k, TopKMap.get(k));
            }
        }
    }
}
