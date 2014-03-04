package de.sec.dns.synthesizer;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.GenerateDataSetTool;
import de.sec.dns.util.Util;

/**
 * The reducer for {@link GenerateDataSetTool}. It fetches entries from the
 * mapper or combiner.
 * 
 * @author Christian Banse
 */
public class SynthesizeDataSetReducer extends Reducer<Text, Text, Text, Text> {
	static int i = 0;

	private static final Log LOG = LogFactory
			.getLog(SynthesizeDataSetReducer.class);
	private static int NUMBER_OF_SYNTHESIZED_DAYS;

	private Calendar c;

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<Text, Text> m;

	private SortedMap<String, String> map = new TreeMap<String, String>();

	private Text one = new Text("" + 1);

	private Text user = new Text();

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
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		user.set(key.toString().trim());

		map.clear();

		for (Text t : values) {
			String date = t.toString().substring(0, 16);
			String value = t.toString().substring(17);

			map.put(date, value);
		}

		if (map.size() < NUMBER_OF_SYNTHESIZED_DAYS) {
			return;
		}

		context.getCounter(SynthesizeDataSetTool.CounterTypes.NUM_CLASSES)
				.increment(1);

		// System.out.println(user);

		try {
			c = Util.getCalendarFromString("1970-01-01-00-00");
		} catch (ParseException e) {
			e.printStackTrace();
		}

		int sessionDuration = context.getConfiguration().getInt(
				Util.CONF_SESSION_DURATION, 1440);

		int sessionStartOffset = context.getConfiguration().getInt(
				Util.CONF_SESSION_OFFSET, Util.DEFAULT_SESSION_OFFSET);

		/*
		 * int i = 0; for(String date : map.keySet()) { if(i >
		 * NUMBER_OF_SYNTHESIZED_DAYS - 1) break; System.out.println("#" + i +
		 * " " + date + ": " + map.get(date)); v.set(map.get(date)); i++;
		 * m.write(user, v, "dataset/1440/tfn/" +
		 * Util.formatDateAccordingToSessionDuration(c, sessionDuration) +
		 * "/data"); c.add(Calendar.MINUTE, sessionDuration); }
		 */

		Random generator = new Random();

		Object[] entries = map.keySet().toArray();
		ArrayList<String> usedDates = new ArrayList<String>();
		String date = null;

		LOG.info("USER " + user);
		for (int i = 0; i < NUMBER_OF_SYNTHESIZED_DAYS; i++) {
			do {
				date = (String) entries[generator.nextInt(entries.length)];
			} while (usedDates.contains(date));

			usedDates.add(date);

			LOG.info("#" + i + " " + date);

			v.set(map.get(date));
			m.write(v,
					null,
					"dataset/1440/tfn/"
							+ Util.formatDateAccordingToSessionDuration(c,
									sessionDuration, sessionStartOffset)
							+ "/data");
			c.add(Calendar.MINUTE, sessionDuration);
		}

		// Im Index muss irgendeine zahl stehen sonst geht es nicht...
		m.write(user, one, "header/" + DataSetHeader.USER_INDEX_DIRECTORY
				+ "/header");
	}

	@Override
	public void setup(Context context) {
		NUMBER_OF_SYNTHESIZED_DAYS = context.getConfiguration().getInt(
				Util.CONF_NUMBER_OF_SYNTHESIZED_DAYS, 0);

		try {
			// initialize the multiple outputs handler
			m = new MultipleOutputs<Text, Text>(context);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
