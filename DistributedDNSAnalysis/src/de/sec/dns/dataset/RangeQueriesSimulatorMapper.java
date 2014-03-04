package de.sec.dns.dataset;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Calendar;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.util.Util;

/**
 * The mapper for {@link RangeQueriesSimulatorMapper}
 * 
 * @author Christian Banse
 */
public class RangeQueriesSimulatorMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Calendar cal;

	private int SESSION_DURATION;

	private int SESSION_OFFSET;

	/**
	 * Random source for dummy selection
	 * 
	 */
	private Random rand = new Random();

	/**
	 * A java crypto digest used to create md5 hashes.
	 */
	private MessageDigest digest;

	/**
	 * Dummy hosts to pick
	 */
	private String[] DUMMY_HOSTS = null;

	/**
	 * Type of range queries; set to true to always generate the same dummies
	 * for a given Host. set to false to generate totally random dummies for
	 * each occurence of a given host.
	 */
	private boolean GENERATE_STATIC_DUMMIES = true;

	/**
	 * Dummy pool size; number of hosts to choose the dummies from
	 */
	private int SIZE_OF_DUMMY_SAMPLES = 100000;

	/**
	 * Number of dummy requests to insert per real request Set to 0 to not
	 * generate dummies.
	 * 
	 */
	private int NUMBER_OF_DUMMIES_PER_REQUEST = 0;

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text k = new Text();

	@Override
	public void map(LongWritable lineKey, Text line, Context context)
			throws IOException, InterruptedException {

		// 1267570240 526 f320625291c110f9bde6aeb9a7c2edfa_WOHNHEIM1
		// 49.233.199.132.in-addr.arpa PTR
		String[] entry = line.toString().split(" ");

		// get rid of unwanted log entries
		// hm or maybe not? or maybe yes?
		if ((entry.length < Util.LOG_ENTRY_INDEX_LENGTH)
		/*
		 * || !Util.sanitize(context, entry[Util.LOG_ENTRY_INDEX_USER],
		 * entry[Util.LOG_ENTRY_INDEX_HOST],
		 * entry[Util.LOG_ENTRY_INDEX_REQUEST_TYPE])
		 */) {
			return;
		}

		// parse the timestamp
		cal.setTimeInMillis(Long
				.parseLong(entry[Util.LOG_ENTRY_INDEX_TIME_SECONDS]
						+ String.format(
								"%03d",
								Integer.parseInt(entry[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS]))));
		Util.adjustCalendarToLocalTime(cal);

		// format the timestamp using pre-defined session durations
		String session = Util.formatDateAccordingToSessionDuration(cal,
				SESSION_DURATION, SESSION_OFFSET);

		// skip invalid dates
		if (!session.startsWith("2010")) {
			return;
		}

		// combine user and session to the key
		// we're using a TextPair to do a secondary sort on the reducer values

		k.set(entry[Util.LOG_ENTRY_INDEX_USER] + "@" + session);
		v.set(line);
		context.write(k, v);

		String host = entry[Util.LOG_ENTRY_INDEX_HOST];

		long seed = 0;
		if (GENERATE_STATIC_DUMMIES) {
			// derive constant seed from md5 hash of hostname
			try {
				byte[] hostHashBytes = new byte[8];
				System.arraycopy(digest.digest(host.getBytes()), 0,
						hostHashBytes, 0, 8);
				ByteArrayInputStream bis = new ByteArrayInputStream(
						hostHashBytes);
				DataInputStream dis = new DataInputStream(bis);
				seed = dis.readLong();

			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}

		if (GENERATE_STATIC_DUMMIES) {
			rand.setSeed(seed);
		}

		for (int i = 0; i < NUMBER_OF_DUMMIES_PER_REQUEST; i++) {
			int randVal = rand.nextInt(SIZE_OF_DUMMY_SAMPLES);
			String dummyHost = DUMMY_HOSTS[randVal];

			StringBuilder builder = new StringBuilder();
			builder.append(entry[Util.LOG_ENTRY_INDEX_TIME_SECONDS] + " "
					+ entry[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS] + " ");
			builder.append(entry[Util.LOG_ENTRY_INDEX_USER] + " ");
			builder.append(dummyHost + " ");
			builder.append(entry[Util.LOG_ENTRY_INDEX_REQUEST_TYPE]);

			k.set(entry[Util.LOG_ENTRY_INDEX_USER] + "@" + session);
			v.set(builder.toString());
			context.write(k, v);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		cal = Calendar.getInstance();
		Configuration conf = context.getConfiguration();

		SESSION_DURATION = conf.getInt(Util.CONF_SESSION_DURATION,
				Util.DEFAULT_SESSION_DURATION);

		SESSION_OFFSET = conf.getInt(Util.CONF_SESSION_OFFSET,
				Util.DEFAULT_SESSION_OFFSET);

		NUMBER_OF_DUMMIES_PER_REQUEST = conf.getInt(
				Util.CONF_NUMBER_OF_DUMMIES_PER_REQUEST, 0);
		GENERATE_STATIC_DUMMIES = conf.getBoolean(
				Util.CONF_GENERATE_STATIC_DUMMIES, true);
		SIZE_OF_DUMMY_SAMPLES = conf.getInt(
				Util.CONF_RANGE_QUERIES_NUMBER_OF_DUMMY_SAMPLES, 100000);

		if (NUMBER_OF_DUMMIES_PER_REQUEST > 0) {
			DUMMY_HOSTS = Util.readDummyHosts(context,
					conf.get(Util.CONF_DUMMY_HOST_PATH), this.getClass());
		}

		// initialize the digest
		try {
			this.digest = MessageDigest.getInstance("MD5");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}