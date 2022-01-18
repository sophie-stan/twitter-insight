import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.lang.String.CASE_INSENSITIVE_ORDER;

public class Triplets {

    public static class CleanTripletsMapper extends Mapper<Text,Text,Text,Text> {
        private final Gson gson = new GsonBuilder().create();

        @Override
        public void map(Text user, Text jsonHashtags, Context context) throws IOException, InterruptedException {
            String[] hashtags = gson.fromJson(jsonHashtags.toString(), String[].class);

            for(String hashtag : hashtags) {
                context.write(new Text(hashtag), new Text(user));
            }
        }
    }

    public static class CleanTripletsReducer extends Reducer<Text, Text, Text, Text> {
        private final Gson gson = new GsonBuilder().create();

        boolean isSizeGreater(Iterable<Text> users) {
            int size = 0;

            for (Text user: users) {
                size++;
                if(size > 10) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected void reduce(Text hashtag, Iterable<Text> users, Context context) throws IOException, InterruptedException {
            if(isSizeGreater(users)) {
                for(Text user: users) {
                    context.write(hashtag, new Text(user));
                }
            }
        }
    }

    public static class HashtagByUserTripletsMapper extends Mapper<Text,Text,Text,Text> {

        @Override
        public void map(Text hashtag, Text user, Context context) throws IOException, InterruptedException {
                context.write(user, hashtag);
        }
    }

    public static class HashtagByUserTripletsReducer extends Reducer<Text, Text, Text, Text> {
        private final Gson gson = new GsonBuilder().create();

        @Override
        protected void reduce(Text user, Iterable<Text> jsonHashtags, Context context) throws IOException, InterruptedException {
            ArrayList<String> hashtags = new ArrayList<>();
            for (Text hashtag : jsonHashtags) {
                hashtags.add(String.valueOf(hashtag));
            }
            context.write(user,new Text(gson.toJson(hashtags)));
        }
    }
    public static class TripletsMapper extends Mapper<Text, Text, Text, Text> {
        private final Gson gson = new GsonBuilder().create();

        @Override
        public void map(Text user, Text jsonHashtags, Context context) throws IOException, InterruptedException {

            // Retrieve data.
            String[] hashtags = gson.fromJson(jsonHashtags.toString(), String[].class);
            int size = hashtags.length;

            // Create the triplets and send to reducer.
            for (int i = 0; i < size - 2; i++) {
                for (int j = i + 1; j < size - 1; j++) {
                    for (int k = j + 1; k < size; k++) {
                        context.write(new Text(tripletToString(hashtags[i], hashtags[j], hashtags[k])), user);
                    }
                }
            }
        }
    }

    public static class TripletsReducer extends Reducer<Text, Text, Text, Text> {
        private final Gson gson = new GsonBuilder().create();

        @Override
        protected void reduce(Text triplet, Iterable<Text> users, Context context) throws IOException, InterruptedException {

            // Add the users to the group.
            SortedSet<String> group = new TreeSet<>(CASE_INSENSITIVE_ORDER); // Theoretically, users will be stored following their username
            for (Text user : users) {
                group.add(user.toString());
            }

            context.write(triplet, new Text(gson.toJson(group)));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.out.println("Usage: hashtagbyuser > userbyhashtag > hashtagbyuserclean > triplets ");
            System.exit(1);
        }

        /* JOB 1 */
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "Clean Triplets");
        job1.setJarByClass(Triplets.class);
        job1.setMapperClass(CleanTripletsMapper.class);
        job1.setReducerClass(CleanTripletsReducer.class);
        job1.setNumReduceTasks(10);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        /* JOB 2 */
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "Clean Triplets 2");
        job2.setJarByClass(Triplets.class);
        job2.setMapperClass(HashtagByUserTripletsMapper.class);
        job2.setReducerClass(HashtagByUserTripletsReducer.class);
        job2.setNumReduceTasks(6);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);

        /* JOB 3 */
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Triplets");
        job.setJarByClass(Triplets.class);
        job.setMapperClass(TripletsMapper.class);
        job.setReducerClass(TripletsReducer.class);
        job.setNumReduceTasks(20); // We only work on one output file coming from the word count

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // String Util
    private static String tripletToString(String first, String second, String third) {
        return "(" + first + ", " + second + ", " + third + ")";
    }
}

