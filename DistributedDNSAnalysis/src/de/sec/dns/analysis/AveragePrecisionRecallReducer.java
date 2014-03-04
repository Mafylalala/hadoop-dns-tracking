package de.sec.dns.analysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Christian Banse
 */
public class AveragePrecisionRecallReducer extends
		Reducer<Text, DoubleWritable, Text, Text> {
	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text k = new Text();

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		String[] rr = key.toString().split("\t");

		String type = rr[0];
		String c = rr[1];

		double sum = 0;
		int total = 0;

		for (DoubleWritable value : values) {
			sum += value.get();
			total += 1;
		}

		double d = sum / total;

		k.set(c);
		v.set(type + "\t" + d);
		context.write(k, v);
	}

	@Override
	public void setup(Context context) {
		try {
			k.set("class");
			v.set("type\tvalue");
			context.write(k, v);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
