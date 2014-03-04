package de.sec.dns.training;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.DataSetInputFormat;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used by the classifier to train
 * instances.
 * 
 * @author Christian Banse
 */
public class TrainingTool extends Configured implements Tool {
	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "Training";

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

		boolean useSynthesize = conf
				.getBoolean(Util.CONF_USE_SYNTHESIZE, false);
		Path datasetPath = new Path(
				useSynthesize ? conf.get(Util.CONF_SYNTHESIZED_DATASET_PATH)
						: conf.get(Util.CONF_DATASET_PATH));

		Path headerPath = new Path(conf.get(Util.CONF_HEADER_PATH));

		String option = conf.get(Util.CONF_OPTIONS);
		String trainingDate = conf.get(Util.CONF_TRAINING_DATE);

		Path outputPath = new Path(conf.get(Util.CONF_TRAINING_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/"); // training
		// date
		// will
		// be
		// appended
		// via
		// multipleoutputs
		// in
		// reducer
		Path inputPath = new Path(datasetPath + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");

		String jobName = Util.JOB_NAME + " [" + TrainingTool.ACTION
				+ "] {dataset=" + datasetPath.getName() + ", header="
				+ headerPath.getName() + ", option=" + option + ", date="
				+ trainingDate + ", session="
				+ conf.get(Util.CONF_SESSION_DURATION) + "}";

		Util.showStatus("Running " + jobName);

		DataSetHeader dataset = new DataSetHeader(conf, headerPath);

		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.setInt(Util.CONF_NUM_ATTRIBUTES, dataset.getNumAttributes());
		conf.setInt(Util.CONF_NUM_CLASSES, dataset.getNumClasses());
		conf.set("mapred.task.timeout", "1800000");
		conf.set(
				"mapred.child.java.opts",
				"-Xmx3000m -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:-UseGCOverheadLimit");
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

		job.setJarByClass(TrainingTool.class);
		job.setMapperClass(TrainingMapper.class);
		job.setReducerClass(TrainingReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleArrayWritable.class);

		job.setInputFormatClass(DataSetInputFormat.class);

		// FileInputFormat.addInputPath(job, inputPath);
		// add input path subdirectories if there are any; otherwise use the
		// path itself

		Calendar firstSession = Util.getCalendarFromString(conf
				.get(Util.CONF_FIRST_SESSION));
		Calendar lastSession = Util.getCalendarFromString(conf
				.get(Util.CONF_LAST_SESSION));

		// if offset-between-training-and-test is used to evaluate offsets of
		// e.g. 2880hrs, the
		// to-Option will have to end accordingly earlier than the end of the
		// dataset.
		// problem is: the confusion matrix needs the trainingdata for all test
		// days, even for the ones
		// behind the to-Option. So we add the offet to the last training
		// session to make sure that the
		// training also happens
		// for the remaining sessions.
		// this is also a problem for the default offset of 1440 - in earlier
		// versions
		// there was special handling to make sure that the "last" session was
		// fed to training
		// we handle this special case with the following approach, too.
		lastSession.add(Calendar.MINUTE, conf.getInt(
				Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
				Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST));

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
							.compareTo(firstSession) >= 0));
					// we need to train the last session because we need a
					// confusion matrix!
					// special case from earlier days; will be taken care of
					// naturally
					// with our new approach of adding the offset between
					// training and
					// test to the lastSession parameter (see above)
					// || cal.equals(lastSession);
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
