package de.sec.dns.analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.test.ConfusionMatrixEntry;
import de.sec.dns.util.Util;

/**
 * @author Christian Banse
 */
public class AveragePrecisionRecallMapper extends
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

		ConfusionMatrixEntry entry = new ConfusionMatrixEntry(line.toString());

		key.set(Util.RECALL_PATH + "\t" + entry.getClassLabel());
		value.set(entry.getRecall());
		context.write(key, value);

		key.set(Util.PRECISION_PATH + "\t" + entry.getClassLabel());
		value.set(entry.getPrecision());
		context.write(key, value);
	}
}
