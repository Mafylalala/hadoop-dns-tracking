package de.sec.dns.dataset;

import java.util.ArrayList;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used by the classifier to train
 * instances.
 * 
 * @author Christian Banse
 */
public class CountEffectiveNumOfAttributesTool extends Configured implements
		Tool {
	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "CountEffectiveNumOfAttributes";

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

		Path datasetPath = new Path(conf.get(Util.CONF_DATASET_PATH));

		Path headerPath = new Path(conf.get(Util.CONF_HEADER_PATH));

		String option = conf.get(Util.CONF_OPTIONS);

		Path outputPath = new Path(conf.get(Util.CONF_HEADER_PATH) + "/"
				+ "effectiveNumOfHostnames_"+conf.get(Util.CONF_COUNT_EFFECTIVE_ATTRIBUTES_OPTION));

		Path inputPath = new Path(datasetPath + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + conf.get(Util.CONF_COUNT_EFFECTIVE_ATTRIBUTES_OPTION) + "/"); // always use raw dataset because we want to be able to count up the actual queries 

		String jobName = Util.JOB_NAME + " ["
				+ CountEffectiveNumOfAttributesTool.ACTION + "] {dataset="
				+ datasetPath.getName() + ", header=" + headerPath.getName()
				+ ", option=" + option + ", session="
				+ conf.get(Util.CONF_SESSION_DURATION) + "}";

		Util.showStatus("Running " + jobName);

		DataSetHeader dataset = new DataSetHeader(conf, headerPath);

		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.setInt(Util.CONF_NUM_ATTRIBUTES, dataset.getNumAttributes());
		conf.setInt(Util.CONF_NUM_CLASSES, dataset.getNumClasses());
		conf.set("mapred.task.timeout", "1800000");
		conf.set(
				"mapred.child.java.opts",
				"-Xmx2500m -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:-UseGCOverheadLimit");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		final int numCores = conf.getInt(Util.CONF_NUM_CORES,
				Util.DEFAULT_NUM_CORES);
		final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
				Util.DEFAULT_NUM_NODES);

		NUM_OF_REDUCE_TASKS = 1; // because we also want to see all hostnames
									// for all instances in the
									// end to come up with the total

		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		job.setJarByClass(CountEffectiveNumOfAttributesTool.class);
		job.setMapperClass(CountEffectiveNumOfAttributesMapper.class);
		job.setReducerClass(CountEffectiveNumOfAttributesReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(DataSetInputFormat.class);

		// FileInputFormat.addInputPath(job, inputPath);
		// add input path subdirectories if there are any; otherwise use the
		// path itself

		Calendar firstSession = Util.getCalendarFromString(conf
				.get(Util.CONF_FIRST_SESSION));
		Calendar lastSession = Util.getCalendarFromString(conf
				.get(Util.CONF_LAST_SESSION));

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
		fs.delete(outputPath, true);

		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 1 : 0;
	}
}
