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
import org.apache.hadoop.util.ToolRunner;

import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used to generate the dataset
 * header out of the raw log files. It uses {@link GenerateDataSetHeaderMapper},
 * {@link GenerateDataSetHeaderCombiner} and
 * {@link GenerateDataSetHeaderReducer} for data processing.
 * 
 * @author Christian Banse
 */
public class DailyDocumentFrequencyTool extends Configured implements Tool {

	/**
	 * An enum which holds counters for {@link GenerateDataSetHeaderReducer}.
	 * 
	 * @author Christian Banse
	 */
	public static enum CounterTypes {
		/**
		 * Counts the number of attributes in the data set.
		 */
		NUM_ATTRIBUTES,

		/**
		 * Counts the number of classes in the data set.
		 */
		NUM_CLASSES,
	}

	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "DailyDocumentFrequency";

	/**
	 * The number of reduce tasks.
	 */
	private static int NUM_OF_REDUCE_TASKS;

	/**
	 * The main entry point if this class is used as an application.
	 * 
	 * @param args
	 *            The command line arguments.
	 * @throws Exception
	 *             if an error occurs.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// read configuration settings from command line
		conf.set(Util.CONF_LOGDATA_PATH, args[0]);
		conf.set(Util.CONF_HEADER_PATH, args[1]);

		// run the tool
		ToolRunner.run(conf, new DailyDocumentFrequencyTool(), args);
	}

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Path headerPath = null;
		Path inputPath = null;

		Configuration conf = getConf();

		final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
				Util.DEFAULT_NUM_NODES);
		NUM_OF_REDUCE_TASKS = numNodes;

		// retrieve our paths from the configuration
		inputPath = new Path(conf.get(Util.CONF_LOGDATA_PATH));
		headerPath = new Path(conf.get(Util.CONF_HEADER_PATH));

		// set the jobname
		String jobName = Util.JOB_NAME + " ["
				+ DailyDocumentFrequencyTool.ACTION + "] {logdata="
				+ inputPath.getName() + ",version=2}";

		Util.showStatus("Running " + jobName);

		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
		// damit die Reducer (v.a. wenn idf gemacht wird) genug RAM bekommen:
		conf.set("mapred.child.java.opts",
				"-Xmx2000M -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");
		conf.set("mapred.reduce.child.java.opts",
				"-Xmx3000M -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");
		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		// set number of reduce tasks
		// MUST BE 1 due to rangeRequestIndex reduce behaviour
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper, combiner and reducer
		job.setJarByClass(DailyDocumentFrequencyTool.class);
		job.setMapperClass(DailyDocumentFrequencyMapper.class);
		job.setReducerClass(DailyDocumentFrequencyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set input and output format
		job.setInputFormatClass(TextInputFormat.class);

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
		fs.delete(headerPath.suffix("/"
				+ DataSetHeader.DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY), true);

		FileOutputFormat.setOutputPath(job, headerPath.suffix("/"
				+ DataSetHeader.DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY));

		// run the job and wait for it to be finished
		boolean b = job.waitForCompletion(true);

		// Delete all empty output files
		Util.deleteEmptyFiles(fs, headerPath.suffix("/"
				+ DataSetHeader.DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY));

		return b ? 1 : 0;
	}
}
