package org.bigdata.finance;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinanceMapper extends Mapper<LongWritable,Text,DoubleWritable,Text> {


	public void map(LongWritable inputKey, Text inputVal, Context context) throws InterruptedException, IOException
	{
		String s=inputVal.toString();
		StringTokenizer splitstring=new StringTokenizer(s,",");
		String[] splits = new String[7];
		int i=0;
		while(splitstring.hasMoreTokens())
		{
			splits[i]=splitstring.nextToken();
			i++;
		}
	
	
	context.write(new DoubleWritable(Double.parseDouble(splits[6])),new Text(splits[0]));
	}
		
	}

	

