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

import javax.naming.Context;

public class HashtagByUser {

    public static class HashtagByUserMapper extends Mapper<LongWritable, TweetWritable, Text, Text> {

        private final Text username = new Text();
        private final Text hashtags = new Text();

        public void map(LongWritable key, TweetWritable value, Context context) {
           //
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "HashtagByUser");
        job.setJarByClass(HashtagByUser.class);
        job.setMapperClass(HashtagByUserMapper.class);
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
