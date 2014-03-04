package de.sec.dns.dataset;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Calendar;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.util.Util;

/**
 * The mapper for {@link GenerateDataSetHeaderTool}. It retrieves the users and
 * hosts from the log file and hands it to the combiner/reducer.
 * 
 * @author Christian Banse
 */
public class GenerateDataSetHeaderMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory
			.getLog(GenerateDataSetHeaderMapper.class);

	private static int ONLY_N_MOST_POPULAR_HOSTNAMES = 0;

	private static int SKIP_N_MOST_POPULAR_HOSTNAMES = 0;

	/**
	 * Add second level domains automatically?
	 */
	private boolean ADD_SECOND_LEVEL_DOMAINS = false;

	private Calendar cal;

	/**
	 * A java crypto MessageDigest used to create md5 hashes
	 */
	private MessageDigest digest;

	private boolean DOWNCASE_HOSTNAMES;

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text k = new Text();

	private Set<String> mostPopularHostnames;
	/**
	 * A temporary variable used for the mapreduce value. Since we handle one
	 * user and host at a time this will always be 1.
	 */
	private final LongWritable one = new LongWritable(1);

	private boolean REJECT_INVALID_HOSTNAMES;

	private int SESSION_DURATION;

	private int SESSION_OFFSET;

	private boolean WRITE_HOST_INDEX_MAPPING;

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
		String[] rr = Util.veryFastSplit(line.toString(), ' ',
				Util.LOG_ENTRY_INDEX_REQUEST_TYPE + 1);

		// get rid of unwanted log entries
		if ((rr.length < 5) || !Util.sanitize(context, rr[2], rr[3], rr[4])) {
			return;
		}

		cal.setTimeInMillis(Long.parseLong(rr[0] + rr[1]));
		Util.adjustCalendarToLocalTime(cal);

		// format the timestamp using pre-defined session durations
		String date = Util.formatDateAccordingToSessionDuration(cal,
				SESSION_DURATION, SESSION_OFFSET);

		// skip invalid dates
		if (!date.startsWith("2010")) {
			return;
		}

		// v.set(1);

		// set the key of the key/value pair. mark it as an user
		k.set(DataSetHeader.USER_INDEX_DIRECTORY + " "
				+ rr[Util.LOG_ENTRY_INDEX_USER]);

		// write the key/value pair
		context.write(k, one);

		String host = rr[3];

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

		k.set(DataSetHeader.REQUEST_RANGE_INDEX_DIRECTORY + " "
				+ rr[Util.LOG_ENTRY_INDEX_USER] + "@" + date);

		context.write(k, one);

		// f�r die Occurence Matrix, die f�r multiple instances ben�tigt
		// wird,
		// nutzen wir die Tatsache, dass f�r den REQUEST_RANGE_INDEX bereits
		// <user>@<date> als Key herausgeschrieben wird.

		// add 2nd level domains; but always skip this step for PTR requests
		String secondLevelDomain = null;

		if (ADD_SECOND_LEVEL_DOMAINS && !rr[4].equals("PTR")) {
			secondLevelDomain = Util.getSecondLevelDomain(host);
		}

		try {
			// create an base64 encoded md5 hash from the host in order to save
			// space and
			// set the key of the key/value pair to mark it as a host
			if (WRITE_HOST_INDEX_MAPPING) {
				k.set(DataSetHeader.HOST_INDEX_DIRECTORY
						+ " "
						+ Util.getHexString(Util.getHash(digest, host)
								.getBytes()) + " " + host);
			} else {
				k.set(DataSetHeader.HOST_INDEX_DIRECTORY
						+ " "
						+ Util.getHexString(Util.getHash(digest, host)
								.getBytes()));
			}
			// write the key/value pair
			context.write(k, one);

			if (ADD_SECOND_LEVEL_DOMAINS && (secondLevelDomain != null)) {
				k.set(DataSetHeader.HOST_INDEX_DIRECTORY + " "
						+ secondLevelDomain);
				context.write(k, one); // wieso wird hier nicht getHexString und
										// getHash gemacht?
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
		try {
			cal = Calendar.getInstance();

			WRITE_HOST_INDEX_MAPPING = context.getConfiguration().getBoolean(
					Util.CONF_WRITE_HOST_INDEX_MAPPING, false);

			ADD_SECOND_LEVEL_DOMAINS = context.getConfiguration().getBoolean(
					Util.CONF_ADD_SECOND_LEVEL_DOMAINS, false);

			DOWNCASE_HOSTNAMES = context.getConfiguration().getBoolean(
					Util.CONF_DOWNCASE_HOSTNAMES,
					Util.DEFAULT_DOWNCASE_HOSTNAMES);

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

			// set up the digest
			digest = MessageDigest.getInstance("MD5");

			int numberOfDummyRequests = context.getConfiguration().getInt(
					Util.CONF_NUMBER_OF_DUMMIES_PER_REQUEST, 0);

			if (numberOfDummyRequests > 0) {
				String[] dummyHosts = Util.readDummyHosts(context, context
						.getConfiguration().get(Util.CONF_DUMMY_HOST_PATH),
						this.getClass());
				for (String host : dummyHosts) {
					k.set(DataSetHeader.HOST_INDEX_DIRECTORY
							+ " "
							+ Util.getHexString(Util.getHash(digest, host)
									.getBytes()) + " " + host);

					context.write(k, one);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}