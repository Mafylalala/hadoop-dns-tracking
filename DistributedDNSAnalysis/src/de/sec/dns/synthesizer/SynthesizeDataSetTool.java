package de.sec.dns.synthesizer;

import java.util.ArrayList;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import de.sec.dns.dataset.GenerateDataSetCombiner;
import de.sec.dns.dataset.GenerateDataSetHeaderReducer;
import de.sec.dns.dataset.GenerateDataSetMapper;
import de.sec.dns.dataset.GenerateDataSetReducer;
import de.sec.dns.test.TestTool;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used to generate the dataset out
 * of the raw log files. It uses {@link GenerateDataSetMapper},
 * {@link GenerateDataSetCombiner} and {@link GenerateDataSetReducer} for data
 * processing.
 * 
 * @author Christian Banse
 */
public class SynthesizeDataSetTool extends Configured implements Tool {
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
		 * Counters the number of classes in the data set.
		 */
		NUM_CLASSES
	}

	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "SynthesizeDataSetTool";

	/**
	 * The number of reduce tasks.
	 */
	private static int NUM_OF_REDUCE_TASKS = 7;

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = null;
		Path outputPath = null;

		if (true)
			throw new Exception(
					"Potentially old code! Please screen it, before you run it");

		Configuration conf = getConf();

		// retrieve our paths from the configuration
		inputPath = new Path(conf.get(Util.CONF_DATASET_PATH));
		outputPath = new Path(conf.get(Util.CONF_SYNTHESIZED_DATASET_PATH));

		// set the jobname
		String jobName = Util.JOB_NAME + " [" + SynthesizeDataSetTool.ACTION
				+ "] {dataset=" + inputPath.getName()
				+ ", synthesized_dataset=" + outputPath.getName()
				+ ", session=" + conf.get(Util.CONF_SESSION_DURATION) + "}";

		Util.showStatus("Running " + jobName);

		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.child.java.opts",
				"-Xmx1500M -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		// set number of reduce tasks

		final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
				Util.DEFAULT_NUM_NODES);
		NUM_OF_REDUCE_TASKS = numNodes;

		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper, combiner and reducer
		job.setJarByClass(TestTool.class);
		job.setMapperClass(SynthesizeDataSetMapper.class);
		// job.setCombinerClass(GenerateDataSetCombiner.class);
		job.setReducerClass(SynthesizeDataSetReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set input and output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setMaxInputSplitSize(job, Util.DATASET_MB_SPLIT * 50);
		FileInputFormat.setMinInputSplitSize(job, Util.DATASET_MB_SPLIT * 50);

		Calendar firstSession = Util.getCalendarFromString(conf
				.get(Util.CONF_FIRST_SYNTHESIZED_SESSION));
		Calendar lastSession = Util.getCalendarFromString(conf
				.get(Util.CONF_LAST_SYNTHESIZED_SESSION));

		// add input path subdirectories if there are any; otherwise use the
		// path itself
		ArrayList<Path> inputPaths = Util.getInputDirectories(fs, inputPath);
		if (inputPaths.size() > 0) {
			for (Path p : inputPaths) {
				Calendar cal = Util.getCalendarFromString(p.getName());
				if (cal.before(lastSession)
						&& (cal.compareTo(firstSession) >= 0)) {
					Util.showStatus("Adding input paths " + p);
					FileInputFormat.addInputPath(job, p);
				}
			}
		} else {
			Util.showStatus("Adding input path " + inputPath);
			FileInputFormat.addInputPath(job, inputPath);
		}

		// clear output dir
		fs.delete(outputPath.suffix("/"/*
										 * +
										 * conf.get(Util.CONF_SESSION_DURATION)
										 */), true);

		FileOutputFormat.setOutputPath(job, outputPath.suffix("/"/*
																 * +
																 * conf.get(Util
																 * .
																 * CONF_SESSION_DURATION
																 * ))
																 */));

		// run the job and wait for it to be completed
		boolean b = job.waitForCompletion(true);

		// retrieve the counters
		/*
		 * Counter numAttributes = job.getCounters().findCounter(
		 * CounterTypes.NUM_ATTRIBUTES);
		 */
		/*
		 * Counter numClasses = job.getCounters().findCounter(
		 * CounterTypes.NUM_CLASSES);
		 * 
		 * // write the counters to the metadata file FSDataOutputStream out =
		 * fs.create(outputPath.suffix("/header/" +
		 * DataSetHeader.META_DATA_FILE)); PrintWriter w = new PrintWriter(out);
		 * 
		 * w.println("relationName=" + header.getName());
		 * w.println("numAttributes=" + header.getNumAttributes());
		 * w.println("numClasses=" + (numClasses.getValue() + 1)); //
		 * UNKNOWN-Klasse
		 * 
		 * w.close(); out.close(); // write UNKNOWN class to index out =
		 * fs.create(outputPath.suffix("/header/" +
		 * DataSetHeader.USER_INDEX_DIRECTORY+"/header-unknownclass")); w = new
		 * PrintWriter(out); w.println("UNKNOWN\t1"); w.close(); out.close();
		 */

		// Delete all empty output files
		Util.deleteEmptyFiles(fs, outputPath);

		return b ? 1 : 0;
	}
}