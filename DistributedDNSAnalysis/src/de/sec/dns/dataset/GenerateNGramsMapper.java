package de.sec.dns.dataset;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.util.TextPair;
import de.sec.dns.util.Util;

/**
 * The mapper for {@link GenerateNGramMapperTool}
 * 
 * @author Christian Banse
 */
public class GenerateNGramsMapper extends
		Mapper<LongWritable, Text, TextPair, Text> {

	private Calendar cal;

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private TextPair k = new TextPair();

	private int SESSION_DURATION;

	private int SESSION_OFFSET;

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

	@Override
	public void map(LongWritable lineKey, Text line, Context context)
			throws IOException, InterruptedException {

		// 1271368462 7 18e_1 v5.lscache1.c.youtube.com A
		// 1271368462 31 18e_1 v5.lscache1.c.youtube.com AAAA
		// 1271368462 633 1 217.5.44.194.in-addr.arpa PTR
		// 1271368462 913 1 217.5.44.194.in-addr.arpa PTR
		// String[] entry = line.toString().split(" ");
		String[] entry = Util.veryFastSplit(line.toString(), ' ',
				Util.LOG_ENTRY_INDEX_REQUEST_TYPE + 1);

		// get rid of unwanted log entries
		if ((entry.length < Util.LOG_ENTRY_INDEX_LENGTH)
				|| !Util.sanitize(context, entry[Util.LOG_ENTRY_INDEX_USER],
						entry[Util.LOG_ENTRY_INDEX_HOST],
						entry[Util.LOG_ENTRY_INDEX_REQUEST_TYPE])) {
			return;
		}

		// skip AAAA if specified
		if (context.getConfiguration().getBoolean(Util.CONF_NGRAMS_SKIP_AAAA,
				Util.DEFAULT_NGRAMS_SKIP_AAAA)
				&& entry[Util.LOG_ENTRY_INDEX_REQUEST_TYPE].equals("AAAA")) {
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
		k.set(entry[Util.LOG_ENTRY_INDEX_USER] + "@" + session,
				entry[Util.LOG_ENTRY_INDEX_TIME_SECONDS]
						+ "."
						+ String.format(
								"%03d",
								Integer.parseInt(entry[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS])));

		v.set(line);
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
	}
}