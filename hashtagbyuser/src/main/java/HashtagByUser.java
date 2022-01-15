import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

import static java.lang.String.CASE_INSENSITIVE_ORDER;

public class HashtagByUser {

    public static class HashtagByUserMapper extends Mapper<LongWritable, TweetWritable, LongWritable, UserWritable> {
        private final Gson gson = new GsonBuilder().create();

        @Override
        public void map(LongWritable key, TweetWritable tweet, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(tweet.userId), new UserWritable(tweet.userName, gson.toJson(tweet.hashtags)));
        }
    }

    public static class HashtagByUserReducer extends Reducer<LongWritable, UserWritable, LongWritable, UserWritable> {
        private final Gson gson = new GsonBuilder().create();

        @Override
        protected void reduce(LongWritable userId, Iterable<UserWritable> values, Context context) throws IOException, InterruptedException {

            SortedSet<String> union = new TreeSet<>(CASE_INSENSITIVE_ORDER);

            // For each tweet of a user.
            String username = null;
            for (UserWritable tweet : values) {
                username = tweet.userName;

                // For each hashtags of a tweet.
                union.addAll(Arrays.asList(gson.fromJson(tweet.hashtags, String[].class)));
            }

            context.write(userId, new UserWritable(username, gson.toJson(union)));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "HashtagByUser");
        job.setJarByClass(HashtagByUser.class);
        job.setMapperClass(HashtagByUserMapper.class);
        job.setCombinerClass(HashtagByUserReducer.class);
        job.setReducerClass(HashtagByUserReducer.class);
        job.setNumReduceTasks(6);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(UserWritable.class);

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
