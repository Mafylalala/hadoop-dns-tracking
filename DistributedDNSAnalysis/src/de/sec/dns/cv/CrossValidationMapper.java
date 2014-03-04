package de.sec.dns.cv;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.util.Util;

public class CrossValidationMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text key = new Text();

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private DoubleWritable value = new DoubleWritable();

	public void map(LongWritable l, Text line, Context context)
			throws IOException, InterruptedException {

		String[] entry = Util.veryFastSplit(line.toString(), '\t', 3);

		// skip header
		if (entry[0].equals("class"))
			return;

		String classLabel = entry[0];
		String type = entry[1];
		double d = Double.parseDouble(entry[2]);

		key.set(classLabel + " " + type);
		value.set(d);
		context.write(key, value);
	}
}
