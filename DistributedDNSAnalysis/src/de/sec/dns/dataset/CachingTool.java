package de.sec.dns.dataset;

import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import de.sec.dns.util.TextPair;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used to filter the raw log files
 * to simulate caching. It uses {@link CachingMapper} and {@link CachingReducer}
 * for data processing.
 * 
 * @author Elmo Randschau
 */
public class CachingTool extends Configured implements Tool {

	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "CachingSimulator";

	/**
	 * The number of reduce tasks.
	 */
	private static int NUM_OF_REDUCE_TASKS;

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = null;
		Path outputPath = null;

		Configuration conf = getConf();

		// retrieve our paths from the configuration
		inputPath = new Path(conf.get(Util.CONF_LOGDATA_PATH));
		outputPath = new Path(conf.get(Util.CONF_CACHING_SIMULATOR_PATH));

		final int numCores = conf.getInt(Util.CONF_NUM_CORES,
				Util.DEFAULT_NUM_CORES);
		final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
				Util.DEFAULT_NUM_NODES);

		NUM_OF_REDUCE_TASKS = numCores * numNodes;

		// set the jobname
		String jobName = Util.JOB_NAME + " [" + CachingTool.ACTION
				+ "] {logdata=" + inputPath.getName() + ", session="
				+ conf.get(Util.CONF_SESSION_DURATION) + "}";

		Util.showStatus("Running " + jobName);

		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.child.java.opts",
				"-Xmx1500M -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");
		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		// set number of reduce tasks
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper, reducer, partitioner and grouping comperator
		job.setJarByClass(CachingTool.class);
		job.setMapperClass(CachingMapper.class);
		job.setReducerClass(CachingReducer.class);
		// GroupingComperator used for Secondary-Sort
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);
		job.setPartitionerClass(TextPair.FirstPartitioner.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(Text.class);

		// set input and output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setMaxInputSplitSize(job, Util.DATASET_MB_SPLIT * 25);
		FileInputFormat.setMinInputSplitSize(job, Util.DATASET_MB_SPLIT * 25);

		// add input path subdirectories if there are any
		ArrayList<Path> inputPaths = Util.getInputDirectories(fs, inputPath);
		int pathsAdded = 0;
		if (inputPaths.size() > 0) {
			for (Path p : inputPaths) {
				if (!p.getName().contains(".") && !p.getName().contains("_")) {
					Util.showStatus("Adding input paths " + p);
					FileInputFormat.addInputPath(job, p);
					pathsAdded++;
				}
			}
		}

		if (pathsAdded == 0) {
			Util.showStatus("Adding input path " + inputPath);
			FileInputFormat.addInputPath(job, inputPath);
		}

		// clear output dir
		fs.delete(
				outputPath.suffix("/" + conf.get(Util.CONF_SESSION_DURATION)),
				true);

		FileOutputFormat.setOutputPath(job,
				outputPath.suffix("/" + conf.get(Util.CONF_SESSION_DURATION)));

		// run the job and wait for it to be completed
		boolean b = job.waitForCompletion(true);

		// NOTE! The counters will be written HERE 
		// retrieve the counters
		Counter numNewInCache = job.getCounters().findCounter(
				CachingReducer.CacheCounter.NEW_TO_CACHE);
		Counter numRenewCache = job.getCounters().findCounter(
				CachingReducer.CacheCounter.RENEW_CACHE);
		Counter numUsedFromCache = job.getCounters().findCounter(
				CachingReducer.CacheCounter.USED_FROM_CACHE);
		
		// write the counters to the metadata file
		Path headerPath = new Path(conf.get(Util.CONF_CACHING_SIMULATOR_PATH));
		FSDataOutputStream out = fs.create(headerPath.suffix("/"
				+ DataSetHeader.SIMULATE_CACHING_METADATA_FILE));
		PrintWriter w = new PrintWriter(out);

		// the sum of all counters equals the sum of all queries in the log file
		w.println("hostnametypeAddedToCache=" + numNewInCache.getValue());
		w.println("queriesAddedAgainToCache=" + numRenewCache.getValue());
		w.println("queriesAnsweredFromCache=" + numUsedFromCache.getValue());

		w.close();
		out.close();		
		
		// Delete all empty output files
		Util.deleteEmptyFiles(fs,
				outputPath.suffix("/" + conf.get(Util.CONF_SESSION_DURATION)));

		return b ? 1 : 0;
	}
}
