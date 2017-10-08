package assignment2.Combiner;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
/*
 * Main class for the Map Reduce job that contains the job configuration
 */
public class App {
  public static void main(String[] args) throws Exception {
	BasicConfigurator.configure();
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: app <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "average temperature calculator");
    job.setJarByClass(App.class);
    job.setMapperClass(TemperatureMapper.class);
    job.setCombinerClass(TemperatureCombiner.class);
    job.setReducerClass(TemperatureReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TemperatureDataWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

