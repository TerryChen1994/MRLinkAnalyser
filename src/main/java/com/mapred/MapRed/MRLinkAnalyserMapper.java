package com.mapred.MapRed;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRLinkAnalyserMapper extends Mapper<Text, Text, Text, Text> {
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
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

		URL keyURL = new URL(key.toString());

		URL valueURL = null;

		String array[] = value.toString().split("\t");
		int anchorLength = 0;

		for (int i = 0; i < array.length; i++) {
			try {
				// アンカーテキスト処理
				if (i % 2 == 0) {
					anchorLength = array[i].split(" ").length;
				}
				// リンク処理
				else {
					a++;
					x += anchorLength;
					valueURL = new URL(array[i]);
					// サイト内
					if (valueURL.getAuthority().equals(keyURL.getAuthority())) {
						b++;
						y += anchorLength;
					}
					// サイト間
					else {
						c++;
						z += anchorLength;
					}
				}
			} catch (Exception err) {
				System.out.println("error url = " + array[i]);
				continue;
			}
		}

		String outputString = a + "\t" + b + "\t" + c + "\t" + x + "\t" + y + "\t" + z;

		long lastProgressTS = 0; // 上一次发心跳的时间点
		long heartBeatInterval = 100000L; // 主动发心跳的间隔，100s，默认600s超时

		if (System.currentTimeMillis() - lastProgressTS > heartBeatInterval) {
			context.progress();
			lastProgressTS = System.currentTimeMillis();
		}
		context.write(key, new Text(outputString));

	}

}
