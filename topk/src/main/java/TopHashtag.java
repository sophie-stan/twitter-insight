import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopHashtag {

    private static final int K = 10;

    public static class MapClass extends Mapper<Text, LongWritable, Text, LongWritable> { // Intwritable ?

        private final TreeMap<Text, LongWritable> TopKMap = new TreeMap<>();

        public void map(Text key, LongWritable value, Context context) {
            TopKMap.put(key, value);

            if (TopKMap.size() > K) {
                TopKMap.remove(TopKMap.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text hashtag : TopKMap.keySet()) {
                context.write(hashtag, TopKMap.get(hashtag));
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
