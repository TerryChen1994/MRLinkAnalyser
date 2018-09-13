package inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import common.ExtendedGZIPInputStream;

public class SkipTextReader extends RecordReader<Text, Text> {
	private long start;
	private long pos;
	private long end;
	private FSDataInputStream fileIn;
	private Text key;
	private Text value;
	private ExtendedGZIPInputStream egzis;
	private boolean continued;

	public SkipTextReader() {

	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		final FileSplit split = (FileSplit) genericSplit;
		final Configuration job = context.getConfiguration();
		this.start = split.getStart();
		this.end = this.start + split.getLength();
		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(job);
		this.fileIn = fs.open(file);

		this.key = new Text();
		this.value = new Text();
		this.continued = false;

		this.egzis = new ExtendedGZIPInputStream(fileIn);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.key = new Text("key");
		this.value = new Text("value");
		label1: while (true) {
			int c = egzis.read();
			if (c == 10 || c == 13) {
				return true;
			} else if (c == -1) {
				break label1;
			} else{
				continue;
			}
		}

		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}
}
