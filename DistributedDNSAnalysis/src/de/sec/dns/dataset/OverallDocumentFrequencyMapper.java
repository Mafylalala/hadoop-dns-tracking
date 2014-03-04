package de.sec.dns.dataset;

import java.io.IOException;
import java.util.Calendar;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.util.Util;

/**
 * The mapper for {@link OverallDocumentFrequencyTool}. It retrieves for each
 * host the user and date from the log file and hands it to the reducer.
 * 
 * @author Dominik Herrmann, Christian Banse
 */
public class OverallDocumentFrequencyMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory
			.getLog(OverallDocumentFrequencyMapper.class);

	private static int ONLY_N_MOST_POPULAR_HOSTNAMES = 0;

	private static int SKIP_N_MOST_POPULAR_HOSTNAMES = 0;

	/**
	 * Add second level domains automatically?
	 */
	private boolean ADD_SECOND_LEVEL_DOMAINS = false;

	private Calendar cal;

	/**
	 * Force all hostnames to lower case?
	 */
	private boolean DOWNCASE_HOSTNAMES;

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text k = new Text();

	private Set<String> mostPopularHostnames;

	private boolean REJECT_INVALID_HOSTNAMES;

	private int SESSION_DURATION;

	private int SESSION_OFFSET;

	/**
	 * A temporary variable used for the mapreduce value. Since we handle one
	 * user and host at a time this will always be 1.
	 */
	private final Text text = new Text();

	/*
	 * public HashMap<String, Integer> getHistogramOfCharacters(String s) {
	 * HashMap<String, Integer> map = new HashMap<String, Integer>(255); for
	 * (int i = 0; i < s.length(); i++) { String c =
	 * Character.toString(s.charAt(i));
	 * 
	 * if (map.containsKey(c)) { int count = map.get(c); map.put(c, new
	 * Integer(count + 1)); } else { map.put(c, 1); } } return map; }
	 */

	@Override
	public void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {
		// 1267570240 526 f320625291c110f9bde6aeb9a7c2edfa_WOHNHEIM1
		// 49.233.199.132.in-addr.arpa PTR
		String[] rr = line.toString().split(" ");

		// get rid of unwanted log entries
		if ((rr.length < 5)
				|| !Util.sanitize(context, rr[Util.LOG_ENTRY_INDEX_USER],
						rr[Util.LOG_ENTRY_INDEX_HOST],
						rr[Util.LOG_ENTRY_INDEX_REQUEST_TYPE])) {
			return;
		}

		String host = rr[Util.LOG_ENTRY_INDEX_HOST];

		if (DOWNCASE_HOSTNAMES) {
			host = host.toLowerCase();
		}

		if (REJECT_INVALID_HOSTNAMES && !Util.isValidHostname(host)) {
			return;
		}

		// get rid of top x hosts in case this feature is enabled
		if (SKIP_N_MOST_POPULAR_HOSTNAMES > 0) {
			if (mostPopularHostnames.contains(host)) {
				return;
			}
		}

		if (ONLY_N_MOST_POPULAR_HOSTNAMES > 0) {
			if (!mostPopularHostnames.contains(host)) {
				return;
			}
		}

		cal.setTimeInMillis(Long
				.parseLong(rr[Util.LOG_ENTRY_INDEX_TIME_SECONDS]
						+ String.format(
								"%03d",
								Integer.parseInt(rr[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS]))));
		Util.adjustCalendarToLocalTime(cal);

		// format the timestamp using pre-defined session durations
		String date = Util.formatDateAccordingToSessionDuration(cal,
				SESSION_DURATION, SESSION_OFFSET);

		// k.set(DataSetHeader.REQUEST_RANGE_INDEX_DIRECTORY + " " + rr[2] + "@"
		// + date);

		// add 2nd level domains; but always skip this step for PTR requests
		String secondLevelDomain = null;

		if (ADD_SECOND_LEVEL_DOMAINS
				&& !rr[Util.LOG_ENTRY_INDEX_REQUEST_TYPE].equals("PTR")) {
			secondLevelDomain = Util.getSecondLevelDomain(host);
		}

		try {
			// create an base64 encoded md5 hash from the host in order to save
			// space and
			// set the key of the key/value pair to mark it as a host

			k.set(DataSetHeader.OVERALL_DOCUMENT_FREQUENCY_INDEX_DIRECTORY
					+ " " + host);
			text.set(rr[Util.LOG_ENTRY_INDEX_USER] + "@" + date);
			context.write(k, text);

			if (ADD_SECOND_LEVEL_DOMAINS && (secondLevelDomain != null)) {
				k.set(DataSetHeader.OVERALL_DOCUMENT_FREQUENCY_INDEX_DIRECTORY
						+ " " + secondLevelDomain);

				text.set(rr[Util.LOG_ENTRY_INDEX_USER] + "@" + date);
				context.write(k, text);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Huffman Index
		// In rr[3] steht der Hostname
		/*
		 * HashMap<String, Integer> map = getHistogramOfCharacters(rr[3]); for
		 * (String mapkey : map.keySet()) { int freq = map.get(mapkey);
		 * k.set(GenerateUserAndHostIndexTool.HUFFMAN_FILE_PREFIX + " " + (int)
		 * mapkey.charAt(0)); v.set(freq); context.write(k, v); }
		 */
	}

	@Override
	public void setup(Context context) {
		cal = Calendar.getInstance();

		ADD_SECOND_LEVEL_DOMAINS = context.getConfiguration().getBoolean(
				Util.CONF_ADD_SECOND_LEVEL_DOMAINS, false);

		DOWNCASE_HOSTNAMES = context.getConfiguration().getBoolean(
				Util.CONF_DOWNCASE_HOSTNAMES, Util.DEFAULT_DOWNCASE_HOSTNAMES);

		REJECT_INVALID_HOSTNAMES = context.getConfiguration().getBoolean(
				Util.CONF_REJECT_INVALID_HOSTNAMES,
				Util.DEFAULT_REJECT_INVALID_HOSTNAMES);

		SESSION_DURATION = context.getConfiguration().getInt(
				Util.CONF_SESSION_DURATION, Util.DEFAULT_SESSION_DURATION);

		SESSION_OFFSET = context.getConfiguration().getInt(
				Util.CONF_SESSION_OFFSET, Util.DEFAULT_SESSION_OFFSET);

		SKIP_N_MOST_POPULAR_HOSTNAMES = context.getConfiguration().getInt(
				Util.CONF_SKIP_N_MOST_POPULAR_HOSTNAMES,
				Util.DEFAULT_SKIP_N_MOST_POPULAR_HOSTNAMES);

		if (SKIP_N_MOST_POPULAR_HOSTNAMES > 0) {
			LOG.info("Skipping " + SKIP_N_MOST_POPULAR_HOSTNAMES
					+ " most popular hostnames. Reading file...");
			try {
				mostPopularHostnames = Util.readTopNHostsFromFile(
						SKIP_N_MOST_POPULAR_HOSTNAMES,
						Util.PATH_MOST_POPULAR_HOSTNAMES,
						context.getConfiguration());
			} catch (IOException e) {
				e.printStackTrace();
			}
			LOG.info("Skipping " + SKIP_N_MOST_POPULAR_HOSTNAMES
					+ " most popular hostnames. Done.");
		} else {
			LOG.info("NOT Skipping most popular hostnames.");
		}

		ONLY_N_MOST_POPULAR_HOSTNAMES = context.getConfiguration().getInt(
				Util.CONF_ONLY_N_MOST_POPULAR_HOSTNAMES,
				Util.DEFAULT_ONLY_N_MOST_POPULAR_HOSTNAMES);
		if (ONLY_N_MOST_POPULAR_HOSTNAMES > 0) {
			LOG.info("Only " + ONLY_N_MOST_POPULAR_HOSTNAMES
					+ " most popular hostnames. Reading file...");
			try {
				mostPopularHostnames = Util.readTopNHostsFromFile(
						ONLY_N_MOST_POPULAR_HOSTNAMES,
						Util.PATH_MOST_POPULAR_HOSTNAMES,
						context.getConfiguration());
			} catch (IOException e) {
				e.printStackTrace();
			}
			LOG.info("Only" + ONLY_N_MOST_POPULAR_HOSTNAMES
					+ " most popular hostnames. Done.");
		} else {
			LOG.info("NOT restricting to only most popular hostnames.");
		}
	}
}