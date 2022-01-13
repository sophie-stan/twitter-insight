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

public class HashtagByUser {

    public static class HashtagByUserMapper extends Mapper<LongWritable, TweetWritable, LongWritable, Text> {

        @Override
        public void map(LongWritable key, TweetWritable value, Context context) throws IOException, InterruptedException {
            String[] hashtags = value.getHashtags();
            context.write(new LongWritable(value.getUserId()), new Text(String.join(",", hashtags)));
        }
    }

    public static class HashtagByUserReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            ArrayList<String> hashtagsList = new ArrayList<>();

            for (Text hashtagsString : values) {
                String[] hashtagsArray = hashtagsString.toString().split(",");
                for (String hashtag: hashtagsArray){
                    //On retire les doublons et chaînes vides
                    if(!Objects.equals(hashtag, "") && !hashtagsList.contains(hashtag)) {
                        hashtagsList.add(hashtag);
                    }
                }
            }
            Collections.sort(hashtagsList, new Comparator<String>() {
                //Redifinition de compare pour trier de manière alphabétique
                @Override
                public int compare(String p1, String p2) {
                    int diff = p1.toLowerCase().compareTo(p2.toLowerCase());
                    if(diff == 0){
                        return - p1.compareTo(p2);
                    }
                    else{
                        return diff;
                    }
                }
            });
            context.write(key, new Text(String.join(",", hashtagsList)));
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

}
