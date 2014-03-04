package de.sec.dns.cv;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.sec.dns.util.Util;

public class CrossValidationInterTool extends Configured implements Tool {

	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "CrossValidationInterIntra";

	/**
	 * The number of reduce tasks.
	 */
	private static int NUM_OF_REDUCE_TASKS = 1;

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
		ToolRunner.run(conf, new CrossValidationInterTool(), args);
	}

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = null;
		Path outputPath = null;

		Configuration conf = getConf();
		String option = conf.get(Util.CONF_OPTIONS);
		// final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
		// Util.DEFAULT_NUM_NODES);
		// NUM_OF_REDUCE_TASKS = numNodes;

		// retrieve our paths from the configuration
		inputPath = new Path(conf.get(Util.CONF_CANDIDATEPATTERN_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");

		outputPath = new Path(conf.get(Util.CONF_INTERINTRA_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");

		// set the jobname
		String jobName = Util.JOB_NAME + " [" + CrossValidationInterTool.ACTION
				+ "] {logdata=" + inputPath.getName() + ",version=2}";

		Util.showStatus("Running " + jobName);

		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		// set number of reduce tasks
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper, combiner and reducer
		job.setJarByClass(CrossValidationInterTool.class);
		job.setMapperClass(CrossValidationInterMapper.class);
		job.setReducerClass(CrossValidationInterReducer.class);

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		ArrayList<Path> inputPaths = Util.getInputDirectories(fs, inputPath);
		boolean include;
		if (inputPaths.size() > 0) {
			for (Path p : inputPaths) {
				if (p.getName().startsWith(".")) {
					continue;
				}
				if (p.getName().startsWith("part-r-")) {
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
		fs.delete(outputPath, true);

		FileOutputFormat.setOutputPath(job, outputPath);

		// run the job and wait for it to be finished
		boolean b = job.waitForCompletion(true);

		// Delete all empty output files
		Util.deleteEmptyFiles(fs, outputPath);

		return b ? 1 : 0;
	}

}
