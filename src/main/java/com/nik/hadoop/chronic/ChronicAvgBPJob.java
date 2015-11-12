package com.nik.hadoop.chronic;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *  hadoop jar target/First-0.0.1-SNAPSHOT.jar com.nik.hadoop.chronic.ChronicAvgBPJob -D mapred.reduce.tasks=1
 *  hadoop fs -cat spark_output/part-r-00000
 *  
 *  hadoop fs -rm -r spark_data/chronic_kidney_disease_full.arff
 *  hadoop fs -put chronic_kidney_disease_full.arff spark_data/chronic_kidney_disease_full.arff
 * */
public class ChronicAvgBPJob extends Configured implements Tool{

	public static class ChronicAvgBPMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		String record;
		private boolean start = false;
		
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
			record = value.toString();
			if (start) {
				String[] fields = record.split(",");
				try {
					context.write(new IntWritable(Integer.valueOf(fields[0])), new IntWritable(Integer.valueOf(fields[1])));	
				} catch(NumberFormatException ex) {
					
				}
			}
			
			if ("@data".equals(record)) {
				start = true;
			}
		}
	} // end of mapper class
	

	public static class ChronicAvgBPReducer extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> {
		
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, FloatWritable>.Context context) throws IOException, InterruptedException {
			Integer s_id = key.get();
			Integer sum = 0;
			Integer cnt = 0;
			
			for (IntWritable value:values) {
				sum = sum + value.get();
				cnt = cnt + 1;
			}
			
			Float avg_m = (float) (sum/cnt);
			context.write(new IntWritable(s_id), new FloatWritable(avg_m));
		}
	} // end of Reducer class
	
	public int run(String[] args) throws Exception {
		final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/chronic_kidney_disease_full.arff");
		final URI OUTPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_output");
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Avg");
		job.setJarByClass(ChronicAvgBPMapper.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ChronicAvgBPMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(ChronicAvgBPReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(INPUT_URI));
		if (FileSystem.get(getConf()).exists(new Path(OUTPUT_URI))) {
			FileSystem.get(getConf()).delete(new Path(OUTPUT_URI), true); // Delete output directory if exists
		}
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_URI));
		
		job.waitForCompletion(true);
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ChronicAvgBPJob(), args);
        System.exit(exitCode);
    }
}