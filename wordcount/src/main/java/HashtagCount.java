import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HashtagCount {

    public static class WordCountMapper extends Mapper<LongWritable, TweetWritable, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(LongWritable key, TweetWritable value, Context context) throws InterruptedException, IOException {

            for (String hashtag : value.hashtags){
                word.set(hashtag);
                context.write(word, one);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "HashtagCount");
        job.setJarByClass(HashtagCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        try {
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        } catch (Exception e) {
            System.out.println("Bad arguments : waiting for 2 arguments [inputURI] [outputURI]");
            return;
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
