package com.mapred.MapRed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MRLinkAnalyserReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// 総リンク数
		double a = 0;
		// サイト内リンク数
		double b = 0;
		// サイト間リンク数
		double c = 0;
		// サイト内リンクのアンカーテキストの平均長さ
		double d = 0;
		// サイト間リンクのアンカーテキストの平均長さ
		double e = 0;
		// 総アンカーテキストの平均長さ
		double f = 0;

		// 総アンカーテキストの長さ
		double x = 0;
		// 総サイト内アンカーテキストの長さ
		double y = 0;
		// 総サイト間アンカーテキストの長さ
		double z = 0;

		for (Text value : values) {
			String array[] = value.toString().split("\t");
			a += Double.valueOf(array[0]);
			b += Double.valueOf(array[1]);
			c += Double.valueOf(array[2]);
			x += Double.valueOf(array[3]);
			y += Double.valueOf(array[4]);
			z += Double.valueOf(array[5]);
		}

		d = ((int) (y / b * 100)) / 100.0;
		e = ((int) (z / c * 100)) / 100.0;
		f = ((int) (x / a * 100)) / 100.0;

		String outputString = a + "\t" + b + "\t" + c + "\t" + d + "\t" + e + "\t" + f;

		context.write(key, new Text(outputString));
	}
}
