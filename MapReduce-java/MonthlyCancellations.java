import java.io.IOException;
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

public class MonthlyCancellations {

    public static class CancellationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text month = new Text();
        private IntWritable isCancelled = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("YEAR")) {
                return;
            }

            String[] columns = value.toString().split(",");

            try {
                if (columns.length > 24 && !columns[24].isEmpty()) {
                    month.set(columns[1]); 
                    int cancelledStatus = (int) Double.parseDouble(columns[24]);
                    
                    if (cancelledStatus == 1) {
                        isCancelled.set(1);
                        context.write(month, isCancelled);
                    }
                }
            } catch (NumberFormatException e) {
                // Ignore rows with invalid formatting
            }
        }
    }

    public static class CancellationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
        Job job = Job.getInstance(conf, "Total Cancellations Per Month");
        
        job.setJarByClass(MonthlyCancellations.class);
        job.setMapperClass(CancellationMapper.class);
        job.setReducerClass(CancellationReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
