import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Average {
    public static class AveMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            
            String line = value.toString(); //2017-11-01,4,0,12 Ave & W 40 St,40.76087502,-74.00277668	1
            String[] keyValue = line.split("\t");
            String scheme = keyValue[0].substring(11); //delete the Date information, keep "4,0,12 Ave & W 40 St,40.76087502,-74.00277668"
            String volume = keyValue[1];
            IntWritable intVolume = new IntWritable(Integer.parseInt(volume));
            context.write(new Text(scheme), intVolume);
        }
    }

    public static class AveReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int totalCount = 0; //count total number of weekday name each month
            int totalVolume = 0;
            for (IntWritable val : values) {
                totalVolume = totalVolume + val.get();
                totalCount = totalCount + 1;
            }
            double aveVolume = (double)totalVolume / (double)totalCount;
            DoubleWritable result = new DoubleWritable(aveVolume);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DataClean");
        job.setJarByClass(DataClean.class);
        job.setMapperClass(AveMapper.class);
        job.setCombinerClass(AveReducer.class);
        job.setReducerClass(AveReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
