package hadoop.samples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountV2 {

  public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      String text = tokenizer.nextToken();
      tokenizer.hasMoreTokens();
      String count = tokenizer.nextToken();
      context.write(new IntWritable(Integer.valueOf(count)), new Text(text));
    }
  }

  public static class Reduce2 extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
      StringBuffer concat = new StringBuffer();
      for (Text val : values) {
        concat.append(val.toString()).append(";");
      }
      context.write(key, new Text(concat.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = new Job(conf, "StringSplitter");
    job1.setMapperClass(Map1.class);
    job1.setReducerClass(Reduce1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    String path = "target/" + System.currentTimeMillis() + "/";
    FileOutputFormat.setOutputPath(job1, new Path(path));

    ControlledJob cJob1 = new ControlledJob(conf);
    cJob1.setJob(job1);

    Configuration conf2 = new Configuration();
    Job job2 = new Job(conf2, "ExchangeKeyValues");
    job2.setMapperClass(Map2.class);
    job2.setReducerClass(Reduce2.class);
    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(path + "part*"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

    ControlledJob cJob2 = new ControlledJob(conf2);
    cJob2.setJob(job2);

    JobControl jobctrl = new JobControl("jobctrl");
    jobctrl.addJob(cJob1);
    jobctrl.addJob(cJob2);
    cJob2.addDependingJob(cJob1);
    jobctrl.run();
  }
}