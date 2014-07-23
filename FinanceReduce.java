package org.bigdata.finance;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FinanceReduce extends Reducer<DoubleWritable,Text,DoubleWritable,Text>{

		public void reduce(DoubleWritable key,Iterable<Text> listofvalues,Context context) throws IOException,InterruptedException
		{
		String sum="";
		for(Text val:listofvalues){
	
		sum=sum+"|"+val.toString();
		
		}
		context.write(key,new Text(sum));
		}
}
