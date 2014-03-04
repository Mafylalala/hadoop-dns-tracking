package de.sec.dns.dataset;

import java.util.ArrayList;
import java.util.Calendar;

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

import de.sec.dns.training.CandidatePatternTool;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used to filter the dataset. It
 * uses {@link FilterDataSetMapper}, {@link FilterDataSetCombiner} and
 * {@link FilterDataSetReducer} for data filtering.
 * 
 * @author Elmo Randschau
 */
public class FilterDataSetTool extends Configured implements Tool {
	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "FilterDataSet";

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
		Path datasetPath = null;
		Path inputPath = null;
		Path outputPath = null;
		Path topPatternPath = null;

		Configuration conf = getConf();
		String option = conf.get(Util.CONF_OPTIONS);

		// retrieve our paths from the configuration
		datasetPath = new Path(conf.get(Util.CONF_DATASET_PATH));
		inputPath = new Path(datasetPath + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");
		outputPath = new Path(conf.get(Util.CONF_DATASET_FILTER_PATH));
		topPatternPath = new Path(conf.get(Util.CONF_CANDIDATEPATTERN_PATH));

		final int numCores = conf.getInt(Util.CONF_NUM_CORES,
				Util.DEFAULT_NUM_CORES);
		final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
				Util.DEFAULT_NUM_NODES);

		// set the jobname
		String jobName = Util.JOB_NAME + " [" + FilterDataSetTool.ACTION
				+ "] {dataset=" + inputPath.getName() + ", TopPattern="
				+ topPatternPath.getName() + ", filter_dataset="
				+ outputPath.getName() + ", session="
				+ conf.get(Util.CONF_SESSION_DURATION) + "}";

		Util.showStatus("Running " + jobName);

		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.child.java.opts",
				"-Xmx1200M -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		// set number of reduce tasks
		NUM_OF_REDUCE_TASKS = numCores * numNodes;
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper, combiner and reducer
		job.setJarByClass(FilterDataSetTool.class);
		job.setMapperClass(FilterDataSetMapper.class);
		job.setReducerClass(FilterDataSetReducer.class);

		job.setMapOutputKeyClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set input and output format
		job.setInputFormatClass(DataSetInputFormat.class);
		job.setOutputFormatClass(DataSetOutputFormat.class);

		// get the first and last Session that will be Analysed... we don't need
		// to filter Sessions
		// which aren't going to be used
		Calendar firstSession = Util.getCalendarFromString(conf
				.get(Util.CONF_FIRST_SESSION));
		Calendar lastSession = Util.getCalendarFromString(conf
				.get(Util.CONF_LAST_SESSION));

		// add input path subdirectories if there are any
		ArrayList<Path> inputPaths = Util.getInputDirectories(fs, inputPath);
		boolean include;

		if (inputPaths.size() > 0) {
			for (Path p : inputPaths) {
				if (p.getName().startsWith("."))
					continue;

				include = false;
				try {
					Calendar cal = Util.getCalendarFromString(p.getName());
					include = (cal.before(lastSession) && (cal
							.compareTo(firstSession) >= 0))
					// we need to train the last session because we need a
					// confusion matrix!
							|| cal.equals(lastSession);
				} catch (Exception ex) {
					if (p.getName().equals("additionalTrainingInstances")) {
						include = true;
					}
				}

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
		fs.delete(
				outputPath.suffix("/" + conf.get(Util.CONF_SESSION_DURATION)),
				true);

		FileOutputFormat.setOutputPath(job,
				outputPath.suffix("/" + conf.get(Util.CONF_SESSION_DURATION)));

		// now we generate the CandidatePatterns prior to filtering...
		// // generate the CandidatePatterns by selection the TopPatterns of
		// each user
		// if (ToolRunner.run(conf, new CandidatePatternTool(), null) == 0)
		// {
		// throw new Exception("CandidatePattern creation failed");
		// }

		// run the job and wait for it to be completed
		boolean b = job.waitForCompletion(true);

		// Delete all empty output files
		Util.deleteEmptyFiles(fs, outputPath);

		// delete the original DataSet to save some space
		fs.delete(
				datasetPath.suffix("/" + conf.get(Util.CONF_SESSION_DURATION)),
				true);

		// set the newly created and filtered DataSet as the DataSet to use
		conf.set(Util.CONF_DATASET_PATH,
				conf.get(Util.CONF_DATASET_FILTER_PATH));
		return b ? 1 : 0;
	}
}
