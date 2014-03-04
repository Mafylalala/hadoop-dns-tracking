package de.sec.dns.dataset;

import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used to generate the dataset
 * header out of the raw log files. It uses {@link GenerateDataSetHeaderMapper},
 * {@link GenerateDataSetHeaderCombiner} and
 * {@link GenerateDataSetHeaderReducer} for data processing.
 * 
 * @author Christian Banse
 */
public class GenerateDataSetHeaderTool extends Configured implements Tool {

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
	private static String ACTION = "GenerateDataSetHeader";

	/**
	 * The number of reduce tasks.
	 * 
	 * MUST BE 1 due to rangeRequestIndex reduce behavior
	 */
	private static int NUM_OF_REDUCE_TASKS = 1;

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Path headerPath = null;
		Path inputPath = null;

		Configuration conf = getConf();

		// retrieve our paths from the configuration
		inputPath = new Path(conf.get(Util.CONF_LOGDATA_PATH));
		headerPath = new Path(conf.get(Util.CONF_HEADER_PATH));

		// set the jobname
		String jobName = Util.JOB_NAME + " ["
				+ GenerateDataSetHeaderTool.ACTION + "] {logdata="
				+ inputPath.getName() + ",version=2}";

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
		// MUST BE 1 due to rangeRequestIndex reduce behaviour
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper, combiner and reducer
		job.setJarByClass(GenerateDataSetHeaderTool.class);
		job.setMapperClass(GenerateDataSetHeaderMapper.class);
		job.setCombinerClass(GenerateDataSetHeaderCombiner.class);
		job.setReducerClass(GenerateDataSetHeaderReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

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
		fs.delete(headerPath, true);

		FileOutputFormat.setOutputPath(job, headerPath);

		// run the job and wait for it to be finished
		boolean b = job.waitForCompletion(true);

		// retrieve the counters
		Counter numAttributes = job.getCounters().findCounter(
				CounterTypes.NUM_ATTRIBUTES);
		Counter numClasses = job.getCounters().findCounter(
				CounterTypes.NUM_CLASSES);

		// write the counters to the metadata file
		FSDataOutputStream out = fs.create(headerPath.suffix("/"
				+ DataSetHeader.META_DATA_FILE));
		PrintWriter w = new PrintWriter(out);

		w.println("relationName=" + inputPath.getName());
		w.println("numAttributes=" + numAttributes.getValue());
		w.println("numClasses=" + numClasses.getValue());

		w.close();
		out.close();

		// Delete all empty output files
		Util.deleteEmptyFiles(fs, headerPath);

		return b ? 1 : 0;
	}
}
