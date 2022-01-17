import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, TweetWritable, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    public void map(LongWritable key, TweetWritable value, Context context) throws InterruptedException, IOException {

        for (String hashtag : value.getHashtags()) {
            word.set(hashtag);
            context.write(word, one);
        }
    }
}