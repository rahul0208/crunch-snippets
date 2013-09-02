package hadoop.samples;

import java.io.IOException;
import java.text.DateFormatSymbols;
import java.util.NavigableMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.crunch.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

// data available at data.gov.in
public class ClimateData {

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        final static String[] months = new DateFormatSymbols().getShortMonths();
        public void map(LongWritable key, Text value, Context context) 
                  throws IOException, InterruptedException {
            String[] parts = StringUtils.split(value.toString());
            if ("YEAR".equals(parts[0])) {
                return;
            }
            String year = parts[0];
            for (int monthCount = 0; monthCount < 12; monthCount++) {
                float rainfall = Float.parseFloat(parts[monthCount + 1]);
                context.write(new Text(months[monthCount] + "," + year), new FloatWritable(rainfall));
            }

        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        TreeMap<Float, String> sortedData = new TreeMap<Float, String>();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException,InterruptedException {
            float totalrainfall = 0;
            for (FloatWritable val : values) {
                totalrainfall += val.get();
            }
            sortedData.put(totalrainfall, key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (float rainfall : sortedData.descendingKeySet()) {
                String string = sortedData.get(rainfall);
                context.write(new Text(string), new FloatWritable(rainfall));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "rainfallData");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}
