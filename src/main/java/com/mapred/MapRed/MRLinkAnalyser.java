package com.mapred.MapRed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import inputformat.RecordInputFormat;
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
	
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPaths(job, "/user/s1721710/statistics");
		FileOutputFormat.setOutputPath(job, new Path("/user/s1721710/result"));
		FileOutputFormat.setCompressOutput(job, true);  //job使用压缩
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(MRLinkAnalyserMapper.class);
		job.setReducerClass(MRLinkAnalyserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
