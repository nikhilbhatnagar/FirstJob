package com.nik.hadoop.chronic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnemicMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	String record;
	private boolean start = false;
	private IntWritable ONE = new IntWritable(1);
	
	//@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
					throws IOException, InterruptedException {
		//super.map(key, value, context);
		
		record = value.toString();
		
		if (start) {
			String[] fields = record.split(",");
			try {
				if("yes".equals(fields[23])) {
					context.write(new IntWritable(Integer.valueOf(fields[0])), ONE);	
				}
			} catch(NumberFormatException ex) {
				
			}
		}
		
		if ("@data".equals(record)) {
			start = true;
		}
	}
}
