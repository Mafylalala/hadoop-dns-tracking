package de.sec.dns.cv;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.sec.dns.dataset.DailyDocumentFrequencyTool;
import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.DataSetInputFormat;
import de.sec.dns.dataset.GenerateDataSetHeaderReducer;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used to generate the Daily
 * Document Frequencies for Cross Validation out of the CrossValidation DataSet
 * Spilts. It uses {@link CrossValidationDDFMapper} and
 * {@link CrossValidationDDFReducer} for data processing.
 * 
 * @author Elmo Randschau
 */
public class CrossValidationDDFTool extends Configured implements Tool {

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
		String option = conf.get(Util.CONF_OPTIONS);
		final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
				Util.DEFAULT_NUM_NODES);
		NUM_OF_REDUCE_TASKS = numNodes;

		// retrieve our paths from the configuration
		inputPath = new Path(conf.get(Util.CONF_DATASET_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");

		headerPath = new Path(conf.get(Util.CONF_HEADER_PATH));

		// set the jobname
		String jobName = Util.JOB_NAME + " [" + CrossValidationDDFTool.ACTION
				+ "] {logdata=" + inputPath.getName() + ",version=2}";

		Util.showStatus("Running " + jobName);

		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		// set number of reduce tasks
		// MUST BE 1 due to rangeRequestIndex reduce behaviour
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper, combiner and reducer
		job.setJarByClass(CrossValidationDDFTool.class);
		job.setMapperClass(CrossValidationDDFMapper.class);
		job.setReducerClass(CrossValidationDDFReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set input and output format
		job.setInputFormatClass(DataSetInputFormat.class);

		ArrayList<Path> inputPaths = Util.getInputDirectories(fs, inputPath);
		boolean include;
		if (inputPaths.size() > 0) {
			for (Path p : inputPaths) {
				if (p.getName().startsWith(".")) {
					continue;
				}
				include = true;

				if (include) {
					Util.showStatus("Adding input paths " + p);
					FileInputFormat.addInputPath(job, p);
				}
			}
		} else {
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
