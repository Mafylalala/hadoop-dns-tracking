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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import de.sec.dns.dataset.DataSetInputFormat;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used by the classifier to train
 * instances.
 * 
 * @author Christian Banse
 */
public class CrossValidationSplitTool extends Configured implements Tool {
	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "CrossValidationSplit";

	/**
	 * The number of reduce tasks.
	 */
	public static int NUM_OF_REDUCE_TASKS;

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		String option = conf.get(Util.CONF_OPTIONS);

		Path cvPath = new Path(conf.get(Util.CONF_CROSS_VALIDATION_PATH));
		Path outputPath = cvPath.suffix("/dataset");

		// our source path is the original data set
		Path sourcePath = new Path(conf.get(Util.CONF_DATASET_PATH));

		Path inputPath = new Path(sourcePath + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");

		String jobName = Util.JOB_NAME + " [" + CrossValidationSplitTool.ACTION
				+ "] {dataset=" + sourcePath.getName() + ", option=" + option
				+ ", session=" + conf.get(Util.CONF_SESSION_DURATION) + "}";

		Util.showStatus("Running " + jobName);

		conf.set("mapred.child.java.opts", "-Xmx1500m -XX:-UseGCOverheadLimit");
		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		final int numCores = conf.getInt(Util.CONF_NUM_CORES,
				Util.DEFAULT_NUM_CORES);
		final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
				Util.DEFAULT_NUM_NODES);

		NUM_OF_REDUCE_TASKS = numCores * numNodes;

		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		job.setJarByClass(CrossValidationSplitTool.class);
		job.setMapperClass(CrossValidationSplitMapper.class);
		job.setReducerClass(CrossValidationSplitReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(DataSetInputFormat.class);

		// FileInputFormat.addInputPath(job, inputPath);
		// add input path subdirectories if there are any; otherwise use the
		// path itself

		ArrayList<Path> inputPaths = Util.getInputDirectories(fs, inputPath);
		if (inputPaths.size() > 0) {
			for (Path p : inputPaths) {
				if (p.getName().startsWith("."))
					continue;

				Util.showStatus("Adding input paths " + p);
				FileInputFormat.addInputPath(job, p);
			}
		} else {
			Util.showStatus("Adding input path " + inputPath);
			FileInputFormat.addInputPath(job, inputPath);
		}

		Util.optimizeSplitSize(job);

		fs.delete(outputPath, true);

		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 1 : 0;
	}
}
