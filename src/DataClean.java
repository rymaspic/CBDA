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

public class DataClean {

    public static class DCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, ParseException{
            String line = value.toString();
            line = line.replace("\"", ""); //clean the "\"" produced by csv
            //257,"2017-11-01 00:04:45","2017-11-01 00:09:02",505,"6 Ave & W 33 St",40.74901271,-73.98848395,477,"W 41 St & 8 Ave",40.75640548,-73.9900262,14860,"Customer",NULL,0
            String[] tokens = line.split(",");

            //Process time
            String oriDate = tokens[2]; // "2017-11-01 00:09:02" end-time
            String[] Date = oriDate.split(" ");
            String format = "yyyy-MM-dd";
            SimpleDateFormat df = new SimpleDateFormat(format);
            java.util.Date date = df.parse(Date[0]); //2017-11-01
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int weekNum = cal.get(Calendar.DAY_OF_WEEK); // 1 represents Monday...
            String weekName = String.valueOf(weekNum);
            String time = Date[1];
            String hour = time.split(":")[0];
            int i = Integer.parseInt(hour); //here I set single time period to be 1-hour
            String timePeriod = String.valueOf(i);

            //Process address
            String address = tokens[4];
            String geo = tokens[5] + "," + tokens[6];//lon,lat

            //Concatenate time and address to be a output key
            List<String> result = new LinkedList<>();
            result.add(Date[0]);
            result.add(weekName);
            result.add(timePeriod);
            result.add(address);
            result.add(geo);
            String finalScheme = String.join(",", result);
            //Year-MM-DD, WeekName, timePeriod, StationName, StationLonLat
            //2017-11-29,4,6,5 St & 6 Ave,40.6704836,-73.98208968

            //Output
            context.write(new Text(finalScheme), one);
             //{"2017-11-29,4,6,5 St & 6 Ave,40.6704836,-73.98208968",1} as {key,value}
        }
    }

    public static class DCReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DataClean");
        job.setJarByClass(DataClean.class);
        job.setMapperClass(DCMapper.class);
        job.setCombinerClass(DCReducer.class);
        job.setReducerClass(DCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

