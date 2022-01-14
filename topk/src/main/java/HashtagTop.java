import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class HashtagTop {

    // Treemap: 2 counts of hashtags equal overwrites one of the hashtags
    private static final int K = 10;

    public static class MapClass extends Mapper<Text, IntWritable, Text, IntWritable> {
        private final TreeMap<Integer, String> TopKMap = new TreeMap<>(Collections.reverseOrder());

        @Override
        public void map(Text hashtag, IntWritable count, Context context) {
            TopKMap.put(count.get(), hashtag.toString());
            if (TopKMap.size() > K) {
                TopKMap.remove(TopKMap.lastKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : TopKMap.entrySet())
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
        }
    }

    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final TreeMap<Integer, String> TopKMap = new TreeMap<>(Collections.reverseOrder());

        @Override
        public void reduce(Text hashtag, Iterable<IntWritable> values, Context context) {
            for (IntWritable count : values) {
                TopKMap.put(count.get(), hashtag.toString());
                if (TopKMap.size() > K) {
                    TopKMap.remove(TopKMap.lastKey());
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : TopKMap.entrySet())
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println("Usage: [sequenceFileInput] [output/input] [textOutput]");
        }

        /* JOB 1 */
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "HashtagCount");
        job1.setJarByClass(HashtagTop.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setNumReduceTasks(1);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        /* JOB 2 */
        Configuration conf2 = new Configuration();

        Job job = Job.getInstance(conf2, "TopHashtag");
        job.setJarByClass(HashtagTop.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setNumReduceTasks(0); // We only work on one output file coming from the word count

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
