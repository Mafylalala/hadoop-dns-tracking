package de.sec.dns.dataset;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.util.HostIndex;
import de.sec.dns.util.Util;

/**
 * The reducer for {@link GenerateDataSetHeaderTool}. It fetches entries from
 * the mapper or combiner.
 * 
 * @author Christian Banse
 */
public class DailyDocumentFrequencyReducer extends
		Reducer<Text, Text, LongWritable, LongWritable> {

	/**
	 * A logger.
	 */
	private static Log LOG = LogFactory
			.getLog(DailyDocumentFrequencyReducer.class);

	/**
	 * 
	 */
	HashMap<String, HashMap<String, Integer>> dateUserRequestNumberMap = new HashMap<String, HashMap<String, Integer>>();

	/**
	 * A java crypto digest used to create md5 hashes.
	 */
	private MessageDigest digest;

	/**
	 * The host index.
	 */
	private HostIndex hostIndex = HostIndex.getInstance();

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

		// <typ> <host> <date>
		String[] rr = key.toString().split(" ");

		if (rr[0]
				.equals(DataSetHeader.DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY)) {

			k.set(hostIndex.getIndexPosition(Util.getHash(digest, rr[1]))); // set
			// key
			// to
			// host
			// determine num of unique classLabels
			Set<String> set = new HashSet<String>();
			for (Text val : values) {
				set.add(val.toString());
			}
			long numberOfInstancesContainingHost = set.size();
			v.set(numberOfInstancesContainingHost); // set value to document
			// frequency of host on
			// given day
			try {
				Util.getCalendarFromString(rr[2]);
				m.write(k, v, rr[2]); // filename format: <headerPrefix>/<date>
			} catch (ParseException e) {
				LOG.info("Skipping record " + rr[1] + "with invalid date "
						+ rr[2]);
			}

		}

		// write the output using MultipleOutputs
		// Host Index is not maintained here any more (binary hostindex is used
		// instead).
		// Therefore, write only user index (see above)
		// m.write(k, v, rr[0] + "/header");
	}

	@Override
	public void setup(Context context) {
		// initialize MultipleOutputs
		m = new MultipleOutputs<LongWritable, LongWritable>(context);

		// initialize the digest
		try {
			this.digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// populate the host index
		if (!hostIndex.isPopulated()) {
			try {
				hostIndex.init(context);
				hostIndex.populateIndex();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
}
