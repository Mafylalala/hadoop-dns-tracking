package de.sec.dns.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.DataSetInputFormat;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * An implementation of an hadoop {@link Tool} used to test a profile based on
 * lift or support
 * 
 * @author Elmo Randschau
 */
public class YangTestTool extends Configured implements Tool {
	/**
	 * An enum which holds counters for {@link TestReducer}.
	 * 
	 * @author Christian Banse
	 */
	public static enum TestCounter {
		/**
		 * Counts the number of correctly classified instances.
		 */
		CORRECTLY_CLASSIFIED,

		/**
		 * Counts the number of not correctly classified instances.
		 */
		NOT_CORRECTLY_CLASSIFIED
	}

	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "Identify";

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

		Path inputPath = new Path(datasetPath + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/"
				+ conf.get(Util.CONF_OPTIONS) + "/"
				+ conf.get(Util.CONF_TEST_DATE));

		Path outputPath = new Path(conf.get(Util.CONF_TEST_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/"
				+ conf.get(Util.CONF_OPTIONS) + "/");

		String jobName = Util.JOB_NAME_YANG + " [" + YangTestTool.ACTION
				+ "] {dataset=" + datasetPath + ", header="
				+ conf.get(Util.CONF_HEADER_PATH) + ", option="
				+ conf.get(Util.CONF_OPTIONS) + ", traindate="
				+ conf.get(Util.CONF_TRAINING_DATE) + ", testdate="
				+ conf.get(Util.CONF_TEST_DATE) + ", session="
				+ conf.get(Util.CONF_SESSION_DURATION) + "}";

		Util.showStatus("Running " + jobName);

		DataSetHeader header = new DataSetHeader(conf, new Path(
				conf.get(Util.CONF_HEADER_PATH)));

		conf.setInt(Util.CONF_NUM_ATTRIBUTES, header.getNumAttributes());
		conf.setInt(Util.CONF_NUM_CLASSES, header.getNumClasses());

		// -XX:HeapDumpPath=/datastore/tmp -XX:+HeapDumpOnOutOfMemoryError
		// hier temporary more MEM assignen!
		conf.set("mapred.child.java.opts",
				"-Xms1500m -Xmx3500m -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");
		conf.set("hadoop.job.ugi", Util.HADOOP_USER);

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, jobName);

		final int numCores = conf.getInt(Util.CONF_NUM_CORES,
				Util.DEFAULT_NUM_CORES);
		final int numNodes = conf.getInt(Util.CONF_NUM_NODES,
				Util.DEFAULT_NUM_NODES);

		NUM_OF_REDUCE_TASKS = numCores * numNodes;

		// set number of reduce tasks
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		// set mapper and reducer
		job.setJarByClass(YangTestTool.class);
		job.setMapperClass(YangTestMapper.class);
		job.setReducerClass(YangTestReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleArrayWritable.class);

		// set input format
		job.setInputFormatClass(DataSetInputFormat.class);

		// set input split
		FileInputFormat.addInputPath(job, inputPath);

		// check if there are additional test instances to consider
		Path additionalTestInstances = inputPath.getParent().suffix(
				"/additionalTestInstances");
		if (fs.exists(additionalTestInstances)) {
			FileInputFormat.addInputPath(job, additionalTestInstances);
		}

		Util.optimizeSplitSize(job);

		// clear output dir
		fs.delete(outputPath, true);

		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 1 : 0;
	}
}
