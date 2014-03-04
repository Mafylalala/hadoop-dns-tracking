package de.sec.dns.cv;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.dataset.DataSetHeader;

/**
 * The Reducer for {@link CrossValidationDDFTool}. It fetches the entries from
 * the mapper and generates the DailyDocumentFrequency Header
 * 
 * @author Elmo Randschau
 */
public class CrossValidationDDFReducer extends
		Reducer<Text, Text, LongWritable, LongWritable> {

	/**
	 * A logger.
	 */
	private static Log LOG = LogFactory.getLog(CrossValidationDDFReducer.class);

	/**
     * 
     */
	HashMap<String, HashMap<String, Integer>> dateUserRequestNumberMap = new HashMap<String, HashMap<String, Integer>>();

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private LongWritable k = new LongWritable();

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<LongWritable, LongWritable> m;

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private LongWritable v = new LongWritable();

	@Override
	public void cleanup(Context context) {
		try {
			m.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// key == <typ> <hostIndex> <date>
		// values == Iterable<Text> classLabels
		String[] rr = key.toString().split(" ");
		String date = rr[2];
		if (rr[0]
				.equals(DataSetHeader.DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY)) {
			// get the hostIndex
			k.set(Long.parseLong(rr[1]));
			long counter = 0l;
			// determine num of unique classLabels
			// Set<String> set = new HashSet<String>();
			for (Text val : values) {
				counter++;
				// set.add(val.toString());
			}

			// set value to document frequency of host on given day
			long numberOfInstancesContainingHost = counter;
			v.set(numberOfInstancesContainingHost);

			m.write(k, v, date); // filename format: <headerPrefix>/<date>
		}
	}

	@Override
	public void setup(Context context) {
		// initialize MultipleOutputs
		try {
			m = new MultipleOutputs<LongWritable, LongWritable>(context);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
