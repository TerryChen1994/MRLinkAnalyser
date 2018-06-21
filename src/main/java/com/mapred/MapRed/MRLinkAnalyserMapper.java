package com.mapred.MapRed;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRLinkAnalyserMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String sv = value.toString();
		String[] svList = sv.split("\t");
		double d1 = Double.valueOf(svList[1]);
		double d2 = Double.valueOf(svList[2]);
		double d3 = Double.valueOf(svList[3]);
		double d4 = Double.valueOf(svList[4]);
		double d5 = Double.valueOf(svList[5]);
		double d6 = Double.valueOf(svList[6]);

		String outputString = d1 + "\t" + d2 + "\t" + d3 + "\t" + d4 + "\t" + d5 + "\t" + d6;
		context.write(new Text("key"), new Text(outputString));
	}
}
