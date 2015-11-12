package com.nik.hadoop.chronic;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *  Total number of anemic people for a age
 *  
 *  hadoop jar target/First-0.0.1-SNAPSHOT.jar com.nik.hadoop.chronic.AnemicJob -D mapred.reduce.tasks=1
 *  hadoop fs -cat spark_output/part-r-00000
 *  
 *  hadoop fs -rm -r spark_data/chronic_kidney_disease_full.arff
 *  hadoop fs -put chronic_kidney_disease_full.arff spark_data/chronic_kidney_disease_full.arff
 * */
public class AnemicJob extends Configured implements Tool {
	public int run(String[] arg0) throws Exception {
		final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/chronic_kidney_disease_full.arff");
		final URI OUTPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_output");
		
		Job job = new Job(getConf());
	    job.setJarByClass(getClass());
	    job.setJobName(getClass().getSimpleName());
		
		// Set input and output file
		FileInputFormat.addInputPath(job, new Path(INPUT_URI));
		if (FileSystem.get(getConf()).exists(new Path(OUTPUT_URI))) {
			FileSystem.get(getConf()).delete(new Path(OUTPUT_URI), true); // Delete output directory if exists
		}
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_URI));
		
		// Set input format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// set mapper & reducer
		job.setMapperClass(AnemicMapper.class);
		job.setReducerClass(IntSumReducer.class);
		
		// set map output
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// set output
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.waitForCompletion(true);
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AnemicJob(), args);
        System.exit(exitCode);
    }
}
