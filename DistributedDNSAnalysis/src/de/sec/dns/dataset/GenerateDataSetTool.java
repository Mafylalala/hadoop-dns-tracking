package de.sec.dns.dataset;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import de.sec.dns.util.TextPair;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used to generate the dataset out
 * of the raw log files. It uses {@link GenerateDataSetMapper},
 * {@link GenerateDataSetCombiner} and {@link GenerateDataSetReducer} for data
 * processing.
 * 
 * @author Christian Banse
 */
public class GenerateDataSetTool extends Configured implements Tool {
	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "GenerateDataSet";

	/**
	 * The number of reduce tasks.
	 */
	// private static int NUM_OF_REDUCE_TASKS = Util.NUMBER_OF_NODES * 1;
	private static int NUM_OF_REDUCE_TASKS;

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Path headerPath = null;
		Path inputPath = null;
		Path outputPath = null;

		Configuration conf = getConf();

		// retrieve our paths from the configuration
		inputPath = new Path(conf.get(Util.CONF_LOGDATA_PATH));
		headerPath = new Path(conf.get(Util.CONF_HEADER_PATH));
		outputPath = new Path(conf.get(Util.CONF_DATASET_PATH));

		final int numCores = conf.getInt(Util.CONF_NUM_CORES,
				Util.DEFAULT_NUM_CORES);
		final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
				Util.DEFAULT_NUM_NODES);

		// set the jobname
		String jobName = Util.JOB_NAME + " [" + GenerateDataSetTool.ACTION
				+ "] {logdata=" + inputPath.getName() + ", header="
				+ headerPath.getName() + ", dataset=" + outputPath.getName()
				+ ", session=" + conf.get(Util.CONF_SESSION_DURATION) + "}";

		Util.showStatus("Running " + jobName);

		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.child.java.opts",
				"-Xmx1200M -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");
		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		// damit die Reducer (v.a. wenn idf gemacht wird) genug RAM bekommen:
		conf.set("mapred.reduce.child.java.opts",
				"-Xmx2700M -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		// set number of reduce tasks
		NUM_OF_REDUCE_TASKS = numCores * numNodes;
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper, combiner and reducer
		job.setJarByClass(GenerateDataSetTool.class);
		job.setMapperClass(GenerateDataSetMapper.class);
		job.setReducerClass(GenerateDataSetReducer.class);

		job.setMapOutputKeyClass(TextPair.class); // key: "date, user"

		boolean useDailyDocFreqs = conf.getBoolean(
				Util.CONF_USE_DAILY_DOCUMENT_FREQUENCIES,
				Util.DEFAULT_USE_DAILY_DOCUMENT_FREQUENCIES);

		if (useDailyDocFreqs) {
			// ensures that the reducers receive keys "date, user" ordered by
			// date, user
			// this is needed for LAZY_READING_OF_IDF=true, which is the default
			// for now
			job.setSortComparatorClass(TextPair.Comparator.class);
		}

		job.setCombinerClass(GenerateDataSetCombiner.class);
		job.setPartitionerClass(TextPair.FirstPartitioner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set input and output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DataSetOutputFormat.class);

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

		Util.optimizeSplitSize(job);

		// clear output dir
		fs.delete(
				outputPath.suffix("/" + conf.get(Util.CONF_SESSION_DURATION)),
				true);

		FileOutputFormat.setOutputPath(job,
				outputPath.suffix("/" + conf.get(Util.CONF_SESSION_DURATION)));

		// run the job and wait for it to be completed
		boolean b = job.waitForCompletion(true);

		// Delete all empty output files
		Util.deleteEmptyFiles(fs, outputPath);

		return b ? 1 : 0;
	}
}