import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * @original author
 * Sophie Stan & Deborah Pereira
 * @modified by David Auber
 */

public class TweetProject {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "DataCleaning.TweetProject");
        job.setJarByClass(TweetProject.class);
        job.setMapperClass(TweetMapper.class);
        /*
         * Map will generate 4,4Mo for each bloc of 128Mo that leeds to 4556 small blocs
         * We must merge (128 / 4,4) small blocs to have one bloc of good size.
         */
        int nbNewBlocs = (int) Math.ceil(4556 / (128 / 4.5));
        job.setNumReduceTasks(nbNewBlocs);
        job.setReducerClass(TweetReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(TweetWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

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
