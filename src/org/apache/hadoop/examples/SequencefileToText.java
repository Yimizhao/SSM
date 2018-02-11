package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SequencefileToText {

	public static class ReaderMapper extends Mapper<Text, IntWritable, Text, Text> {

		protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
//			context.write(key, new Text(value + ""));
			context.write(key, null);
		}
	}

//	public static class WriteReducer extends Reducer<Text, Text, Text, Text> {
//
//		protected void reduce(Text key, Iterator<Text> values, Context context)
//				throws IOException, InterruptedException {
//			while (values.hasNext()) {
//				context.write(key, values.next());
//			}
//		}
//	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new String[] { "hdfs://192.168.83.244:50040/user/cxd/output", "hdfs://192.168.83.244:50040/user/cxd/out" };
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage : SequencefileToText ");
			System.exit(2);
		}
		Job job = new Job(conf, "SequencefileToText");
		job.setJarByClass(SequencefileToText.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		// section2
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		// section3
		job.setMapperClass(ReaderMapper.class);
//		 job.setCombinerClass(WriteReducer.class);
//		 job.setReducerClass(WriteReducer.class);

		// section4
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		Path output = new Path(otherArgs[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);

		// section5
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}