package de.sec.dns.dataset;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.sec.dns.util.HostIndex;
import de.sec.dns.util.Util;

/**
 * The reducer for {@link OverallDocumentFrequencyTool}. It fetches entries from
 * the mapper.
 * 
 * @author Dominik Herrmann, Christian Banse
 */
public class OverallDocumentFrequencyReducer extends
		Reducer<Text, Text, LongWritable, LongWritable> {

	/**
	 * A logger.
	 */
	private static Log LOG = LogFactory
			.getLog(OverallDocumentFrequencyReducer.class);

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
	 * A temporary variable used for the mapreduce value.
	 */
	private LongWritable v = new LongWritable();

	@Override
	public void cleanup(Context context) {
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// <indextyp> <host>
		String[] rr = key.toString().split(" ");

		if (rr[0]
				.equals(DataSetHeader.OVERALL_DOCUMENT_FREQUENCY_INDEX_DIRECTORY)) {

			k.set(hostIndex.getIndexPosition(Util.getHash(digest, rr[1]))); // set
			// key
			// to
			// host

			// determine num of unique sessions (format of values is
			// "<user>@<date>")
			Set<String> set = new HashSet<String>();
			for (Text val : values) {
				set.add(val.toString());
			}

			long numberOfInstancesContainingHost = set.size();
			if (numberOfInstancesContainingHost == 0) {
				throw new InterruptedException(
						"Fatal error! NumberOfInstancesContainingHost is 0 for host "
								+ rr[1]);
			} else if (numberOfInstancesContainingHost == 1) {
				// do not write entries for hosts which occur only once
				// (time/memory tradeoff)
				return;
			}

			v.set(numberOfInstancesContainingHost); // set value to document
			// frequency of host on
			// given day
			context.write(k, v);
		}

	}

	@Override
	public void setup(Context context) {

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
