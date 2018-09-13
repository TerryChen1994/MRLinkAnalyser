package com.mapred.MapRed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MRLinkAnalyserReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for (Text value : values) {
			count++;
		}
		String outValue = count + "";
		context.write(key, new Text(outValue));
	}
}
