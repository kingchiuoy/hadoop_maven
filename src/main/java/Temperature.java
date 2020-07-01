
// cc MaxTemperatureWithCombiner Application to find the maximum temperature, using a combiner function for efficiency
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
//import org.apache.hadoop.;
//import org.apache.hadoop.



public class Temperature {

    public static class TemMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private double max = Double.MIN_VALUE;
        private double min = Double.MAX_VALUE;

        @Override
        protected void map(LongWritable key, Text value, Context context) {

            String line = value.toString();
            if (line == null || line.equals("")) {
                return;
            }
            String[] splits = line.split(",");

            try {
                double temp = Double.parseDouble(splits[2]);
                max = Math.max(temp, max);
                min = Math.min(temp, min);
            } catch (NumberFormatException ignored) {

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("max"), new DoubleWritable(max));
            context.write(new Text("min"), new DoubleWritable(min));
        }

    }

    public static class TemReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private double max = Double.MIN_VALUE;
        private double min = Double.MAX_VALUE;

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("min")) {
                for (DoubleWritable value : values)
                    min = Math.min(value.get(), min);
                context.write(new Text("min"), new DoubleWritable(min));
            } else if (key.toString().equals("max")) {
                for (DoubleWritable value : values)
                    max = Math.max(value.get(), max);
                context.write(new Text("max"), new DoubleWritable(max));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Min Max Temperature");
        job.setJarByClass(Temperature.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(TemMapper.class);
        job.setReducerClass(TemReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
// ^^ MaxTemperature