package de.sec.dns.cv;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.sec.dns.util.Util;

/**
 * @author Christian Banse
 */
public class CrossValidationReducer extends
		Reducer<Text, DoubleWritable, Text, Text> {
	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text k = new Text();

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

	private double overallPrecision = 0;
	private double overallRecall = 0;

	private int i = 0;

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		String[] rr = Util.veryFastSplit(key.toString(), ' ', 2);

		String classLabel = rr[0];
		String type = rr[1];

		double sum = 0;
		int total = 0;

		for (DoubleWritable value : values) {
			sum += value.get();
			total += 1;
		}

		double d = sum / total;

		if (type.equals("precision")) {
			overallPrecision += d;
			i++;
		}

		if (type.equals("recall")) {
			overallRecall += d;
		}

		k.set(classLabel);
		v.set(type + "\t" + d);
		context.write(k, v);
	}

	@Override
	public void cleanup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();

		overallPrecision /= i;
		overallRecall /= i;

		Path analysisPath = new Path(conf.get(Util.CONF_ANALYSIS_PATH));
		FileSystem fs = FileSystem.get(conf);

		FSDataOutputStream out = fs.create(analysisPath
				.suffix("/overallPrecisionRecall"));
		PrintWriter w = new PrintWriter(out);

		w.println("type\tvalue");
		w.println("precision\t" + overallPrecision);
		w.println("recall\t" + overallRecall);
		w.close();
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
