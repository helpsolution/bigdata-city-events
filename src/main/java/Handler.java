import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import types.LogWritable;

import java.io.IOException;


/**
 * The entry point for the Handler
 * which setup the Hadoop job with Maps and Reduce Classes
 *
 * @author Chernilin
 */
public class Handler extends Configured implements Tool {

    /**
     * Map Class which extends mapreduce.Mapper class
     * It handle log files
     * @author Chernilin
     */
    public static class LogsMapper extends Mapper<Object, Text, Text, LogWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text city = new Text();
        private LogWritable logWr = new LogWritable(new Text("value"), new Text(), one);

        private Integer biddingPrice = null;
        private String cityID = "";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String record = value.toString();
            String[] parts = record.split("\t|\n|\r|\f");

            biddingPrice = new Integer(parts[19]);
            cityID = parts[7];
            if (biddingPrice > 250) {
                city.set(cityID);
                context.write(city, logWr);
            }
        }
    }

    /**
     * Map Class which extends mapreduce.Mapper class
     * It handle city info files
     * @author Chernilin
     */
    public static class CityInfoMapper extends Mapper<Object, Text, Text, LogWritable> {

        private LogWritable logWr = new LogWritable(new Text("name"), new Text(), new IntWritable());

        private Text cityID;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String record = value.toString();
            String[] parts = record.split("\t| ");

            cityID = new Text(parts[0]);
            logWr.setName(new Text(parts[1]));

            context.write(cityID, logWr);
        }
    }


    /**
     * Reduce class which is executed after the map class and takes
     * key and corresponding values, sums all the values, replaces id to names and write the
     * name along with the corresponding total occurances in the output
     *
     * @author Chernilin
     */
    public static class ReduceJoinReducer extends Reducer<Text, LogWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        /**
         * Method which performs the join operation and sums
         */
        public void reduce(Text key, Iterable<LogWritable> values, Context context) throws IOException, InterruptedException {

            String name = "";
            int sum = 0;
            for (LogWritable t : values)
            {
                if (t.getType().toString().equals("value"))
                {
                    sum += Integer.parseInt(t.getValue().toString());
                }
                else if (t.getType().toString().equals("name"))
                {
                    name = t.getName().toString();
                }
            }
            if (sum!= 0) {
                result.set(sum);
                context.write(new Text(name), result);
            }
        }
    }


    /**
     * Run method which schedules the Hadoop Job
     * @param args Arguments passed in main function
     */
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Handler job");

        job.setJarByClass(Handler.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LogWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(3);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, LogsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, CityInfoMapper.class);

        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Main function which calls the run method and passes the args using ToolRunner
     * @param args Three arguments input and output file paths
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Handler(), args);
        System.exit(res);
    }

}