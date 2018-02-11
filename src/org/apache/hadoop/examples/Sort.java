/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.examples;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.security.jgss.ExtendedGSSContext;

/**
 * This is the trivial map/reduce program that does absolutely nothing other
 * than use the framework to fragment and sort the input values.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar sort [-r <i>reduces</i>]
 * [-inFormat <i>input format class</i>] [-outFormat <i>output format class</i>]
 * [-outKey <i>output key class</i>] [-outValue <i>output value class</i>]
 * [-totalOrder <i>pcnt</i> <i>num samples</i> <i>max splits</i>] <i>in-dir</i>
 * <i>out-dir</i>
 */
public class Sort<K, V> extends Configured implements Tool {
	public static final String REDUCES_PER_HOST = "mapreduce.sort.reducesperhost";
	private Job job = null;

	static int printUsage() {
		System.out.println(
				"sort [-r <reduces>] " + "[-inFormat <input format class>] " + "[-outFormat <output format class>] "
						+ "[-outKey <output key class>] " + "[-outValue <output value class>] "
						+ "[-totalOrder <pcnt> <num samples> <max splits>] " + "<input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return 2;
	}

	public static class SortMapper extends Mapper<Object, Text, NullWritable, Text> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(null, word);
			}
		}
	}

	public static class SortReduce extends Reducer<NullWritable, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {

				context.write(null, value);
			}
		}
	}

	/**
	 * The main driver for sort program. Invoke this method to submit the map/reduce
	 * job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		JobClient client = new JobClient(conf);
		ClusterStatus cluster = client.getClusterStatus();
		int num_reduces = (int) (cluster.getMaxReduceTasks() * 0.9);
		String sort_reduces = conf.get(REDUCES_PER_HOST);
		if (sort_reduces != null) {
			num_reduces = cluster.getTaskTrackers() * Integer.parseInt(sort_reduces);
		}
		Class<? extends InputFormat> inputFormatClass = TextInputFormat.class;
		Class<? extends OutputFormat> outputFormatClass = TextOutputFormat.class;
		Class<? extends WritableComparable> outputKeyClass = Text.class;
		Class<? extends Writable> outputValueClass = NullWritable.class;
		List<String> otherArgs = new ArrayList<String>();
		InputSampler.Sampler<K, V> sampler = null;
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-r".equals(args[i])) {
					num_reduces = Integer.parseInt(args[++i]);
				} else if ("-inFormat".equals(args[i])) {
					inputFormatClass = Class.forName(args[++i]).asSubclass(InputFormat.class);
				} else if ("-outFormat".equals(args[i])) {
					outputFormatClass = Class.forName(args[++i]).asSubclass(OutputFormat.class);
				} else if ("-outKey".equals(args[i])) {
					outputKeyClass = Class.forName(args[++i]).asSubclass(WritableComparable.class);
				} else if ("-outValue".equals(args[i])) {
					outputValueClass = Class.forName(args[++i]).asSubclass(Writable.class);
				} else if ("-totalOrder".equals(args[i])) {
					double pcnt = Double.parseDouble(args[++i]);
					int numSamples = Integer.parseInt(args[++i]);
					int maxSplits = Integer.parseInt(args[++i]);
					if (0 >= maxSplits)
						maxSplits = Integer.MAX_VALUE;
					sampler = new InputSampler.RandomSampler<K, V>(pcnt, numSamples, maxSplits);
				} else {
					otherArgs.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				return printUsage(); // exits
			}
		}
		// Set user-supplied (possibly default) job configs
		job = Job.getInstance(conf);
		job.setJobName("sorter");
		job.setJarByClass(Sort.class);

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReduce.class);

		job.setNumReduceTasks(num_reduces);

		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(outputFormatClass);

		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);

		// Make sure there are exactly 2 parameters left.
		if (otherArgs.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " + otherArgs.size() + " instead of 2.");
			return printUsage();
		}
		FileInputFormat.setInputPaths(job, otherArgs.get(0));

		Path output = new Path(otherArgs.get(1));
		FileSystem fS = FileSystem.get(conf);
		if (fS.exists(output)) {
			fS.delete(output, true);
		}

		FileOutputFormat.setOutputPath(job, output);

		if (sampler != null) {
			System.out.println("Sampling input to effect total-order sort...");
			job.setPartitionerClass(TotalOrderPartitioner.class);
			Path inputDir = FileInputFormat.getInputPaths(job)[0];
			FileSystem fs = inputDir.getFileSystem(conf);
			inputDir = inputDir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
			Path partitionFile = new Path(inputDir, "_sortPartitioning");
			TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
			InputSampler.<K, V>writePartitionFile(job, sampler);
			URI partitionUri = new URI(partitionFile.toString() + "#" + "_sortPartitioning");
			job.addCacheFile(partitionUri);
		}

		System.out.println("Running on " + cluster.getTaskTrackers() + " nodes to sort from "
				+ FileInputFormat.getInputPaths(job)[0] + " into " + FileOutputFormat.getOutputPath(job) + " with "
				+ num_reduces + " reduces.");
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		int ret = job.waitForCompletion(true) ? 0 : 1;
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");
		return ret;
	}

	public static void main(String[] args) throws Exception {
		args = new String[] { "hdfs://192.168.83.244:50040/user/cxd/input",
				"hdfs://192.168.83.244:50040/user/cxd/output" };
		int res = ToolRunner.run(new Configuration(), new Sort(), args);
		System.exit(res);
	}

	//
	/**
	 * Get the last job that was run using this instance.
	 * 
	 * @return the results of the last job that was run
	 */
	public Job getResult() {
		return job;
	}
}
