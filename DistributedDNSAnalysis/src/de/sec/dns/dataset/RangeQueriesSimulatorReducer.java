package de.sec.dns.dataset;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The reducer for {@link RangeQueriesSimulatorTool}. It fetches entries from
 * the mapper or combiner.
 * 
 * @author Christian Banse
 */
public class RangeQueriesSimulatorReducer extends
		Reducer<Text, Text, NullWritable, Text> {
	/**
	 * Counts how many entries have already been reduced.
	 */
	private static int counter = 0;

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private NullWritable k = NullWritable.get();

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<NullWritable, Text> m;

	@Override
	public void cleanup(Context context) {
		try {
			// close the multiple output handler
			m.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(Text userAtSession, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		// String[] rr = userAtSession.toString().split("@");
		// String session = rr[1];

		for (Text value : values) {
			// m.write(k, value, session + "/logs");
			context.write(k, value);
		}
	}

	@Override
	public void setup(Context context) {
		try {
			// initialize the multiple outputs handler
			// m = new MultipleOutputs<NullWritable, Text>(context);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
