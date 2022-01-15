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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.lang.String.CASE_INSENSITIVE_ORDER;

public class Triplets {

    public static class TripletsMapper extends Mapper<Text, Text, Text, Text> {
        private final Gson gson = new GsonBuilder().create();

        @Override
        public void map(Text user, Text jsonHashtags, Context context) throws IOException, InterruptedException {

            // Retrieve data.
            String[] hashtags = gson.fromJson(jsonHashtags.toString(), String[].class);
            int size = hashtags.length;

            // Attempt to limit the number of combinations....
            if (size > 10) {
                size = 10;
            }

            // Create the triplets and send to reducer.
            for (int i = 0; i < size; i++) {
                for (int j = i + 1; j < size; j++) {
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

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Triplets");
        job.setJarByClass(Triplets.class);
        job.setMapperClass(TripletsMapper.class);
//        job.setCombinerClass(TripletsReducer.class);
        job.setReducerClass(TripletsReducer.class);
        job.setNumReduceTasks(20);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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

    // String Util
    private static String tripletToString(String first, String second, String third) {
        return "(" + first + ", " + second + ", " + third + ")";
    }
}

