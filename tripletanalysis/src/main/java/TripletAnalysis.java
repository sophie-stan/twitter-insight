import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class TripletAnalysis {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Usage: triplets > tripletcount");
            System.exit(1);
        }

        /* JOB 1 */
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "Count Triplets");
        job1.setJarByClass(TripletAnalysis.class);
        job1.setMapperClass(TokenCounterMapper.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setNumReduceTasks(38);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}

