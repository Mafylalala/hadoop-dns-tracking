package de.sec.dns.dataset;

import java.io.IOException;
import java.util.Calendar;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.util.TextPair;
import de.sec.dns.util.Util;

/**
 * The mapper for {@link GenerateDataSetTool}. It retrieves the users and hosts
 * from the log file and hands it to the combiner/reducer.
 * 
 * Output Key: TextPair(date, user)
 * 
 * @author Christian Banse
 */
public class GenerateDataSetMapper extends
		Mapper<LongWritable, Text, TextPair, Text> {

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory
			.getLog(GenerateDataSetMapper.class);

	private Calendar cal;

	private boolean DOWNCASE_HOSTNAMES;

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private TextPair k = new TextPair();

	private Set<String> mostPopularHostnames;

	private int ONLY_N_MOST_POPULAR_HOSTNAMES;

	private boolean REJECT_INVALID_HOSTNAMES;

	private boolean REJECT_WINDOWS_BROWSER_QUERIES;

	private int SESSION_DURATION;

	private int SESSION_OFFSET;

	private int SKIP_N_MOST_POPULAR_HOSTNAMES;

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

	@Override
	public void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {

		// 1267570240 526 f320625291c110f9bde6aeb9a7c2edfa_WOHNHEIM1
		// 49.233.199.132.in-addr.arpa PTR
		String[] entry = Util.veryFastSplit(line.toString(), ' ',
				Util.LOG_ENTRY_INDEX_REQUEST_TYPE + 1);

		// get rid of unwanted log entries
		if ((entry.length < Util.LOG_ENTRY_INDEX_LENGTH)
				|| !Util.sanitize(context, entry[Util.LOG_ENTRY_INDEX_USER],
						entry[Util.LOG_ENTRY_INDEX_HOST],
						entry[Util.LOG_ENTRY_INDEX_REQUEST_TYPE])) {
			return;
		}

		String host = entry[Util.LOG_ENTRY_INDEX_HOST];

		if (REJECT_WINDOWS_BROWSER_QUERIES && Util.isWindowsBrowserQuery(host)) {
			return;
		}

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

		// parse the timestamp
		cal.setTimeInMillis(Long
				.parseLong(entry[Util.LOG_ENTRY_INDEX_TIME_SECONDS]
						+ String.format(
								"%03d",
								Integer.parseInt(entry[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS]))));
		Util.adjustCalendarToLocalTime(cal);

		// format the timestamp using pre-defined session durations
		String date = Util.formatDateAccordingToSessionDuration(cal,
				SESSION_DURATION, SESSION_OFFSET);

		// skip invalid dates
		if (!date.startsWith("2010")) {
			return;
		}

		// combine date and user to the key
		k.set(date, entry[Util.LOG_ENTRY_INDEX_USER]);
		v.set(host);
		context.write(k, v);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		cal = Calendar.getInstance();
		SESSION_DURATION = context.getConfiguration().getInt(
				Util.CONF_SESSION_DURATION, Util.DEFAULT_SESSION_DURATION);

		SESSION_OFFSET = context.getConfiguration().getInt(
				Util.CONF_SESSION_OFFSET, Util.DEFAULT_SESSION_OFFSET);

		DOWNCASE_HOSTNAMES = context.getConfiguration().getBoolean(
				Util.CONF_DOWNCASE_HOSTNAMES, Util.DEFAULT_DOWNCASE_HOSTNAMES);

		REJECT_INVALID_HOSTNAMES = context.getConfiguration().getBoolean(
				Util.CONF_REJECT_INVALID_HOSTNAMES,
				Util.DEFAULT_REJECT_INVALID_HOSTNAMES);

		REJECT_WINDOWS_BROWSER_QUERIES = context.getConfiguration().getBoolean(
				Util.CONF_REJECT_WINDOWS_BROWSER_QUERIES,
				Util.DEFAULT_REJECT_WINDOWS_BROWSER_QUERIES);

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