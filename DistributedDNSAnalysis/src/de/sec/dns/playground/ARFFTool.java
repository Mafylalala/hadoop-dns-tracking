package de.sec.dns.playground;

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
import org.apache.hadoop.util.ToolRunner;

import de.sec.dns.test.TestTool;
import de.sec.dns.util.Util;

public class ARFFTool extends Configured implements Tool {
	public static String ACTION = "GenerateDataSet";

	public static int NUM_OF_REDUCE_TASKS = 7;
	static long startTime = System.currentTimeMillis();

	public static void main(String[] args) throws Exception {
		showStatus("Starting...");
		int res = ToolRunner.run(new Configuration(), new ARFFTool(), args);
		showStatus("Finished with status: " + res);
	}

	public static void showStatus(String status) {
		System.out.println("@"
				+ ((System.currentTimeMillis() - startTime) / 1000)
				+ "s | "
				+ (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
						.freeMemory()) / (1000 * 1000) + " mb | " + status);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path headerPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		int numOfReduceTasks = NUM_OF_REDUCE_TASKS;

		if (args.length >= 4) {
			numOfReduceTasks = Integer.parseInt(args[3]);
		}

		showStatus("InputPath: " + inputPath);
		showStatus("HeaderPath: " + headerPath);
		showStatus("OuputPath: " + outputPath);
		showStatus("NumOfReduceTasks: " + numOfReduceTasks);

		Configuration conf = getConf();

		conf.set("mapred.child.java.opts",
				"-Xmx1G -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode");
		conf.set(Util.CONF_HEADER_PATH, headerPath.toString());

		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, Util.JOB_NAME + " [" + ARFFTool.ACTION
				+ "] {in=" + inputPath.getName() + ", header="
				+ headerPath.getName() + ", out=" + outputPath.getName() + "}");

		job.setNumReduceTasks(numOfReduceTasks);

		job.setJarByClass(TestTool.class);
		job.setMapperClass(ARFFMapper.class);
		job.setCombinerClass(ARFFCombiner.class);
		job.setReducerClass(ARFFReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(ARFFOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);

		// clear output dir
		fs.delete(outputPath, true);

		FileOutputFormat.setOutputPath(job, outputPath);

		boolean b = job.waitForCompletion(true);

		// Delete all part-r- files because they're empty anyway
		Util.deleteEmptyFiles(fs, outputPath);

		return b ? 1 : 0;
	}
}