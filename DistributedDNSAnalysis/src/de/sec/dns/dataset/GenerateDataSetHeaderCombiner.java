package de.sec.dns.dataset;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.sec.dns.util.Util;

/**
 * The combiner for {@link GenerateDataSetHeaderTool}. It fetches entries from
 * the mapper and starts combining the entries.
 * 
 * @author Christian Banse
 */
public class GenerateDataSetHeaderCombiner extends
		Reducer<Text, LongWritable, Text, LongWritable> {

	/**
	 * A temporary variable which holds the value of the entry.
	 */
	private LongWritable v = new LongWritable();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		// sum up all entries
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
			Util.ping(context, GenerateDataSetHeaderCombiner.class);
		}

		// set the value
		v.set(sum);

		// write it to the reducer
		context.write(key, v);
	}
}
