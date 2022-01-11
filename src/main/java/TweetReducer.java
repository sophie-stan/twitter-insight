/**
 * @original author
 * David Auber
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TweetReducer extends Reducer<LongWritable, TweetWritable, LongWritable, TweetWritable> {
    public void reduce(LongWritable key, Iterable<TweetWritable> values, Context context)
            throws IOException, InterruptedException {
        long date = -1;
        TweetWritable tweet = null;
        for (TweetWritable val : values) {
            if (val.timestamp > date) {
                date = val.timestamp;
                tweet = val.clone();
            }            
        }
        context.write(key, tweet);
    }
}
