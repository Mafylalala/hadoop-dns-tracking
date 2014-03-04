package de.sec.dns.analysis;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used by the classifier to test
 * instances against a defined training set.
 * 
 * @author Christian Banse
 */
public class AveragePrecisionRecallTool extends Configured implements Tool {
	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "AveragePrecisionRecall";

	/**
	 * The number of reduce tasks.
	 */
	private static int NUM_OF_REDUCE_TASKS = 1;

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Path outputPath = new Path(conf.get(Util.CONF_ANALYSIS_PATH) + "/"
				+ "averagePrecisionRecallPerUser");

		Path inputPath = new Path(conf.get(Util.CONF_MATRIX_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/"
				+ conf.get(Util.CONF_OPTIONS));

		String jobName = Util.JOB_NAME + " ["
				+ AveragePrecisionRecallTool.ACTION + "]";

		Util.showStatus("Running " + jobName);

		DataSetHeader header = new DataSetHeader(conf, new Path(
				conf.get(Util.CONF_HEADER_PATH)));

		conf.setInt(Util.CONF_NUM_ATTRIBUTES, header.getNumAttributes());
		conf.setInt(Util.CONF_NUM_CLASSES, header.getNumClasses());

		conf.set("mapred.child.java.opts",
				"-Xms1500m -Xmx1700m -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");
		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		// set number of reduce tasks
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper and reducer
		job.setJarByClass(AveragePrecisionRecallTool.class);
		job.setMapperClass(AveragePrecisionRecallMapper.class);
		job.setReducerClass(AveragePrecisionRecallReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// set input format
		job.setInputFormatClass(TextInputFormat.class);

		int sessionOffset = conf.getInt(
				Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
				Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST);

		Calendar firstTestSession = Util.getCalendarFromString(conf
				.get(Util.CONF_FIRST_SESSION));
		firstTestSession.add(Calendar.MINUTE, sessionOffset);

		Calendar lastTestSession = Util.getCalendarFromString(conf
				.get(Util.CONF_LAST_SESSION));

		ArrayList<Path> inputPaths = Util.getInputDirectories(fs, inputPath);
		if (inputPaths.size() > 0) {
			for (Path p : inputPaths) {
				Calendar cal = Util.getCalendarFromString(p.getName());
				if ((cal.before(lastTestSession) && (cal
						.compareTo(firstTestSession) >= 0))
						|| cal.equals(lastTestSession)) {
					Util.showStatus("Adding input paths " + p);
					FileInputFormat.addInputPath(job, p);
				}
			}
		} else {
			Util.showStatus("Adding input path " + inputPath);
			FileInputFormat.addInputPath(job, inputPath);
		}

		// clear output dir
		fs.delete(outputPath, true);

		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 1 : 0;
	}
}
