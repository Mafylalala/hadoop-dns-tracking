package de.sec.dns.dataset;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.util.TextPair;
import de.sec.dns.util.Util;

/**
 * The mapper for {@link CachingTool}
 * 
 * @author Elmo Randschau
 */
public class CachingMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	Configuration conf;
	private Calendar cal;

	private int SESSION_DURATION;

	private int SESSION_OFFSET;

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private TextPair k = new TextPair();

	@Override
	public void map(LongWritable lineKey, Text line, Context context)
			throws IOException, InterruptedException {
		// 1271368462 7 18e_1 v5.lscache1.c.youtube.com A
		// 1271368462 31 18e_1 v5.lscache1.c.youtube.com AAAA
		// 1271368462 633 1 217.5.44.194.in-addr.arpa PTR
		// 1271368462 913 1 217.5.44.194.in-addr.arpa PTR
		String[] entry = line.toString().split(" ");

		// parse the timestamp
		cal.setTimeInMillis(Long
				.parseLong(entry[Util.LOG_ENTRY_INDEX_TIME_SECONDS]
						+ String.format(
								"%03d",
								Integer.parseInt(entry[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS]))));
		
		Util.adjustCalendarToLocalTime(cal);
		String session = Util.formatDateAccordingToSessionDuration(cal,
				SESSION_DURATION, SESSION_OFFSET);

		// skip invalid dates
		if (!session.startsWith("2010")) {
			return;
		}


		// Caching-Duration starts with each user Session
		if (conf.get(Util.CONF_CACHING_SIMULATOR,
				Util.CONF_CACHING_WITHINSESSION).equals(
				Util.CONF_CACHING_WITHINSESSION)) {

			k.set(entry[Util.LOG_ENTRY_INDEX_USER] + "@" + session,
					entry[Util.LOG_ENTRY_INDEX_TIME_SECONDS]
							+ "."
							+ String.format(
									"%03d",
									Integer.parseInt(entry[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS])));
		}
		// Caching-Duration can overlap a user Session, hence no Session
		// information is passed to the
		// reducer
		else if ((conf.get(Util.CONF_CACHING_SIMULATOR,
				Util.CONF_CACHING_FIXEDDURATION)
				.equals(Util.CONF_CACHING_FIXEDDURATION))
				|| ((conf.get(Util.CONF_CACHING_SIMULATOR,
						Util.CONF_CACHING_FIXEDDURATION)
						.equals(Util.CONF_CACHING_SLIDINGWINDOW)))) {

			k.set(entry[Util.LOG_ENTRY_INDEX_USER],
					entry[Util.LOG_ENTRY_INDEX_TIME_SECONDS]
							+ "."
							+ String.format(
									"%03d",
									Integer.parseInt(entry[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS])));

		}

		v.set(line);
		context.write(k, v);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		cal = Calendar.getInstance();
		conf = context.getConfiguration();

		SESSION_DURATION = conf.getInt(Util.CONF_SESSION_DURATION,
				Util.DEFAULT_SESSION_DURATION);

		SESSION_OFFSET = conf.getInt(Util.CONF_SESSION_OFFSET,
				Util.DEFAULT_SESSION_OFFSET);
	}

}
