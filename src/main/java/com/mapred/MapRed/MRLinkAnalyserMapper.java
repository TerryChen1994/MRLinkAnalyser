package com.mapred.MapRed;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRLinkAnalyserMapper extends Mapper<Text, Text, Text, Text> {
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, new Text());
	}

}
