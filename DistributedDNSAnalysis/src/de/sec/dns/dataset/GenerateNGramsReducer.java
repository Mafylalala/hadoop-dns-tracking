package de.sec.dns.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.util.TextPair;
import de.sec.dns.util.Util;

/**
 * The reducer for {@link GenerateDataSetTool}. It fetches entries from the
 * mapper or combiner.
 * 
 * @author Christian Banse
 */
public class GenerateNGramsReducer extends
		Reducer<TextPair, Text, NullWritable, Text> {
	/**
	 * Counts how many entries have already been reduced.
	 */
	private static int counter = 0;

	private static int GRAMSIZE;
	private static boolean ADD_LOWER_NGRAMS;

	private static String SPACE_STRING = " ";
	private static String COMMA_STRING = ",";

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private NullWritable k = NullWritable.get();

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<NullWritable, Text> m;

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

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
	protected void reduce(TextPair pair, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String[] userInSession = pair.getFirst().toString().split("@");
		String user = userInSession[0];
		String session = userInSession[1];

		HashMap<Integer, ArrayList<String>> prevHosts = new HashMap<Integer, ArrayList<String>>();
		HashMap<Integer, ArrayList<String>> prevReqTypes = new HashMap<Integer, ArrayList<String>>();

		for (int gramsize = (ADD_LOWER_NGRAMS ? 1 : GRAMSIZE); gramsize <= GRAMSIZE; gramsize++) {
			prevHosts.put(gramsize, new ArrayList<String>());
		}

		for (int gramsize = (ADD_LOWER_NGRAMS ? 1 : GRAMSIZE); gramsize <= GRAMSIZE; gramsize++) {
			prevReqTypes.put(gramsize, new ArrayList<String>());
		}

		// String[] prevHosts = new String[GRAMSIZE - 1];
		// String[] prevReqType = new String[GRAMSIZE - 1];

		String[] requestStartInMilliseconds = Util.veryFastSplit(pair.getSecond().toString(),'.',2);
		String requestStart = requestStartInMilliseconds[0] + " " + requestStartInMilliseconds[1];
		
		int i = 0;

		for (Text line : values) {
			// 1267570240 526 f320625291c110f9bde6aeb9a7c2edfa_WOHNHEIM1
			// 49.233.199.132.in-addr.arpa PTR
			String[] entry = Util.veryFastSplit(line.toString(), ' ',
					Util.LOG_ENTRY_INDEX_REQUEST_TYPE + 1);
			String host = entry[Util.LOG_ENTRY_INDEX_HOST];
			String reqType = entry[Util.LOG_ENTRY_INDEX_REQUEST_TYPE];

			for (int gramsize = (ADD_LOWER_NGRAMS ? 1 : GRAMSIZE); gramsize <= GRAMSIZE; gramsize++) {
				if (i >= gramsize - 1) {
					StringBuilder builder = new StringBuilder();
					builder.append(requestStart);
					builder.append(SPACE_STRING);
					builder.append(user);
					builder.append(SPACE_STRING);

					for (String prevHost : prevHosts.get(gramsize)) {
						builder.append(prevHost);
						builder.append(COMMA_STRING);
					}
					builder.append(host);
					builder.append(SPACE_STRING);

					for (String prevReqType : prevReqTypes.get(gramsize)) {
						builder.append(prevReqType);
						builder.append(COMMA_STRING);
					}
					builder.append(reqType);

					v.set(builder.toString());
					m.write(k, v, session + "/logs");

					// i = 0;
					if (gramsize == GRAMSIZE) {
						requestStart = entry[Util.LOG_ENTRY_INDEX_TIME_SECONDS]
								+ " "
								+ entry[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS];
					}
				}
			}

			for (int gramsize = (ADD_LOWER_NGRAMS ? 2 : GRAMSIZE); gramsize <= GRAMSIZE; gramsize++) {
				prevHosts.get(gramsize).add(entry[Util.LOG_ENTRY_INDEX_HOST]);
				prevReqTypes.get(gramsize).add(
						entry[Util.LOG_ENTRY_INDEX_REQUEST_TYPE]);

				if (prevHosts.get(gramsize).size() == gramsize) {
					prevHosts.get(gramsize).remove(0);
					prevReqTypes.get(gramsize).remove(0);
				}
			}
			i++;

			if ((counter++ % 1000) == 0) {
				Util.ping(context, GenerateNGramsReducer.class);
			}
		}
	}

	@Override
	public void setup(Context context) {
		try {
			Configuration conf = context.getConfiguration();

			GRAMSIZE = conf.getInt(Util.CONF_NGRAMS_SIZE,
					Util.DEFAULT_NGRAMS_SIZE);

			ADD_LOWER_NGRAMS = conf.getBoolean(Util.CONF_NGRAMS_ADD_LOWER,
					Util.DEFAULT_NGRAMS_PRESERVE_1GRAMS);

			// initialize the multiple outputs handler
			m = new MultipleOutputs<NullWritable, Text>(context);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
