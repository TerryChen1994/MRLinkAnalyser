package com.mapred.MapRed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MRLinkAnalyserReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// 単一総リンク数
		double d1 = 0;
		// 単一サイト内リンク数
		double d2 = 0;
		// 単一サイト間リンク数
		double d3 = 0;
		// 単一サイト内リンクのアンカーテキストの平均長さ
		double d4 = 0;
		// 単一サイト間リンクのアンカーテキストの平均長さ
		double d5 = 0;
		// 単一アンカーテキストの平均長さ
		double d6 = 0;

		// 総url数
		double a = 0;
		// サイト内リンクを含むurl数
		double b = 0;
		// サイト間リンクを含むurl数
		double c = 0;
		// サイト内とサイト間リンク両方を含むurl数
		double d = 0;
		// 総リンク数
		double e = 0;
		// 総サイト内リンク数
		double f = 0;
		// 総サイト間リンク数
		double g = 0;
		// サイト内アンカー平均長さ
		double h = 0;
		// サイト間アンカー平均長さ
		double i = 0;
		// アンカー平均長さ
		double j = 0;

		// サイト内とサイト間リンクいずれかを含むurl数
		double tag = 0;
		double x = 0;
		double y = 0;

		for (Text value : values) {
			String array[] = value.toString().split("\t");
			d1 += Double.valueOf(array[0]);
			d2 += Double.valueOf(array[1]);
			d3 += Double.valueOf(array[2]);
			x += Double.valueOf(array[3]);
			y += Double.valueOf(array[4]);
			d4 += x;
			d5 += y;
			d6 += Double.valueOf(array[5]);
			
			a++;
			if (x > 0 && y > 0) {
				b++;
				c++;
				d++;
				tag++;
			} else if (x > 0) {
				b++;
				tag++;
			} else if (y > 0) {
				c++;
				tag++;
			}

		}
		e = d1;
		f = d2;
		g = d3;

		h = ((int) (d4 / b * 100)) / 100.0;
		i = ((int) (d5 / c * 100)) / 100.0;
		j = ((int) (d6 / tag * 100)) / 100.0;

		String outputString = a + "\t" + b + "\t" + c + "\t" + d + "\t" + e + "\t" + f + "\t" + g + "\t" + h + "\t" + i
				+ "\t" + j;

		context.write(key, new Text(outputString));
	}
}
