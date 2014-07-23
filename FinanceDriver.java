package org.bigdata.finance;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class FinanceDriver extends Configured implements Tool{


		public static void main(String args[]) throws Exception
		{
			ToolRunner.run(new FinanceDriver(), args);
		}
		
		
		public int run(String[] args) throws Exception
		{
			Job job=new Job(getConf(),"Stock High Job");
			job.setJarByClass(FinanceDriver.class);
			job.setMapperClass(FinanceMapper.class);
			job.setReducerClass(FinanceReduce.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
			return 0;
		}
}
