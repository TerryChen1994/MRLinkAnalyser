package com.mapred.MapRed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import inputformat.LongTextInputFormat;
import outputformat.AnchorURLOutputFormat;

public class MRLinkAnalyser {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.reduce.child.java.opts", "-Xmx1024m");
		conf.set("mapred.map.child.java.opts", "-Xmx2048m");
		conf.setBoolean("mapred.compress.map.output", true);
		conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		conf.setDouble("mapred.job.shuffle.input.buffer.percent", 0.50);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(MRLinkAnalyser.class);
		job.setJobName("MRLinkAnalyser");
	
		job.setNumReduceTasks(100);
		
		String prePath = "/user/s1721710/Index/index";
		String allPath = new String();
		for(int i = 0; i < 20; i++){
			String curS = String.valueOf(i);
			if(i < 10){
				curS = "0" + curS;
			}
			String curPath = prePath + curS;
			if(i != 19){
				allPath = allPath + curPath + ",";
			}else{
				allPath = allPath + curPath;
			}
		}
		
		FileInputFormat.addInputPaths(job, allPath);
		FileOutputFormat.setOutputPath(job, new Path("/user/s1721710/statistics"));
		FileOutputFormat.setCompressOutput(job, true);  //job使用压缩
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(MRLinkAnalyserMapper.class);
		job.setReducerClass(MRLinkAnalyserReducer.class);
		job.setInputFormatClass(LongTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
