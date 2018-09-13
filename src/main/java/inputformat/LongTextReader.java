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

public class LongTextReader extends RecordReader<Text, Text> {
	private long start;
	private long pos;
	private long end;
	private FSDataInputStream fileIn;
	private Text key;
	private Text value;
	private ExtendedGZIPInputStream egzis;
	private boolean continued;

	public LongTextReader() {

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
		
		StringBuilder outKey = new StringBuilder();
		StringBuilder outValue = new StringBuilder();

		if (!continued) {
			key = new Text();
			label1: while (true) {
				int c = egzis.read();
				if (c == -1) {
					outKey.setLength(0);
					break label1;
				} else if (c == 10 || c == 13) {
					outKey.setLength(0);
					break label1;
				} else if (c == 9) {
					break label1;
				} else {
					outKey.append((char) c);
				}
			}
			if (outKey.length() > 0) {
				label2: while (true) {
					int count = 0;
					label3: while (true) {
						int c = egzis.read();
						if (c == -1) {
							break label2;
						} else if (c == 9) {
							if (count == 0) {
								count++;
							} else {
								outValue.append((char) c);
								count = 0;
								break label3;
							}
						} else if (c == 10 || c == 13 && count == 1) {
							continued = false;
							this.key = new Text(outKey.toString());
							this.value = new Text(outValue.toString().trim());
							outKey.setLength(0);
							outValue.setLength(0);
							count = 0;
							return true;
						} else if (c == 10 || c == 13 && count == 0) {
							throw new IOException("c == 10 || c == 13 && count == 0");
						}
						outValue.append((char) c);
					}
					if (outValue.length() > 10000000) {
						continued = true;
						this.key = new Text(outKey.toString());
						this.value = new Text(outValue.toString().trim());
						outKey.setLength(0);
						outValue.setLength(0);
						return true;
					}
				}
			}
		} else if (continued) {
			label2: while (true) {
				int count = 0;
				label3: while (true) {
					int c = egzis.read();
					if (c == -1) {
						break label2;
					} else if (c == 9) {
						if (count == 0) {
							count++;
						} else {
							outValue.append((char) c);
							count = 0;
							break label3;
						}
					} else if (c == 10 || c == 13 && count == 1) {
						continued = false;
						this.value = new Text(outValue.toString().trim());
						outKey.setLength(0);
						outValue.setLength(0);
						count = 0;
						return true;
					} else if (c == 10 || c == 13 && count == 0) {
						throw new IOException("c == 10 || c == 13 && count == 0");
					}
					outValue.append((char) c);
				}
				if (outValue.length() > 10000000) {
					continued = true;
					this.value = new Text(outValue.toString().trim());
					outKey.setLength(0);
					outValue.setLength(0);
					return true;
				}
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
