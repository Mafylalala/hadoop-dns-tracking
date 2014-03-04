package de.sec.dns.test;

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

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used to generate the confusion
 * matrix of a test result produces by {@link TestTool}.
 * 
 * @author Christian Banse
 */
public class ConfusionMatrixTool extends Configured implements Tool {
	/**
	 * The action used by the job name.
	 */
	public static String ACTION = "ConfusionMatrix";

	/**
	 * The number of reduce tasks. Set to 1 in order to reduce the number of
	 * files.
	 */
	public static int NUM_OF_REDUCE_TASKS = 1;

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Path testPath = new Path(conf.get(Util.CONF_TEST_PATH));

		String option = conf.get(Util.CONF_OPTIONS);
		String testDate = conf.get(Util.CONF_TEST_DATE);
		String headerPath = conf.get(Util.CONF_HEADER_PATH);

		// TODO: might cause fault?
		Path inputPath = testPath.suffix("/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/"
				+ Util.PREDICTED_CLASSES_PATH + "/" + testDate);
		Path outputPath = new Path(conf.get(Util.CONF_MATRIX_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/"
				+ testDate);

		// set the jobname
		String jobName = Util.JOB_NAME + " [" + ConfusionMatrixTool.ACTION
				+ "] {test=" + testPath.getName() + ", option=" + option
				+ ", testdate=" + testDate + ", session="
				+ conf.get(Util.CONF_SESSION_DURATION) + "}";

		Util.showStatus("Running " + jobName);

		// read the header because we need to number of classes
		DataSetHeader header = new DataSetHeader(conf, new Path(headerPath));

		conf.setInt(Util.CONF_NUM_CLASSES, header.getNumClasses());
		conf.set("mapred.child.java.opts", "-Xmx1G");
		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set the mapper and reducer
		job.setJarByClass(ConfusionMatrixTool.class);
		job.setMapperClass(ConfusionMatrixMapper.class);
		job.setReducerClass(ConfusionMatrixReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);

		// clear output dir
		fs.delete(outputPath, true);

		FileOutputFormat.setOutputPath(job, outputPath);

		job.submit();
		// return 1;
		return job.waitForCompletion(true) ? 1 : 0;
	}
}
