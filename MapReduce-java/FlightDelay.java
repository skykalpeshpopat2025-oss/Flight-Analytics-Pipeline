import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightDelay {

    public static class DelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text airline = new Text();
        private IntWritable delay = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("YEAR")) {
                return;
            }

            String[] columns = value.toString().split(",");

            try {
                if (columns.length > 22 && !columns[22].isEmpty()) {
                    airline.set(columns[4]); 
                    int delayMinutes = Integer.parseInt(columns[22]);
                    delay.set(delayMinutes);
                    context.write(airline, delay);
                }
            } catch (NumberFormatException e) {
                // Ignore rows with invalid or missing delay numbers
            }
        }
    }

    public static class DelayReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long count = 0;

            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                double average = (double) sum / count;
                result.set(average);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Flight Delay");
        
        job.setJarByClass(FlightDelay.class);
        job.setMapperClass(DelayMapper.class);
        job.setReducerClass(DelayReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
