package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TextToSequencefile {

	public static class ReaderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class WriterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		protected void reduce(Text key, Iterator<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			while (values.hasNext()) {
				sum += ((IntWritable) values.next()).get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// section 1
		Configuration conf = new Configuration();
		args = new String[] { "hdfs://192.168.83.244:50040/user/cxd/input", "hdfs://192.168.83.244:50040/user/cxd/inputSequencefile" };
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage : TextToSequencefile ");
			System.exit(2);
		}
		Job job = new Job(conf, "TextToSequencefile");
		job.setJarByClass(TextToSequencefile.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// SequenceFileOutputFormat.setOutputCompressionType(job,CompressionType.NONE);
		// section2
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// section3
		job.setMapperClass(ReaderMapper.class);
		job.setCombinerClass(WriterReducer.class);
		job.setReducerClass(WriterReducer.class);

		// section4
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		Path output = new Path(otherArgs[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		SequenceFileOutputFormat.setOutputPath(job, output);

		// section5
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}