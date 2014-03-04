package de.sec.dns.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.util.Util;

/**
 * The reducer for {@link GenerateDataSetHeaderTool}. It fetches entries from
 * the mapper or combiner.
 * 
 * @author Christian Banse
 */
public class GenerateDataSetHeaderReducer extends
		Reducer<Text, LongWritable, Text, Text> {

	/**
	 * A logger.
	 */
	private static Log LOG = LogFactory
			.getLog(GenerateDataSetHeaderReducer.class);

	/**
	 * 
	 */
	HashMap<String, HashMap<String, Integer>> dateUserRequestNumberMap = new HashMap<String, HashMap<String, Integer>>();

	/**
	 * f�r die occurrence Matrix m�ssen wir uns merken, welche User an
	 * welchen Tagen aktiv waren.
	 */
	HashMap<String, HashMap<String, Integer>> usersDateMap = new HashMap<String, HashMap<String, Integer>>();

	/**
	 * 
	 */

	private FSDataOutputStream fos;

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text k = new Text();

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<Text, Text> m;
	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

	private boolean WRITE_HOST_INDEX_MAPPING;

	@Override
	public void cleanup(Context context) {
		LOG.info("CLEANUP; Writing Request Range Index");
		try {
			for (String date : dateUserRequestNumberMap.keySet()) {
				Set<String> usersAtDate = dateUserRequestNumberMap.get(date)
						.keySet();
				int[] requestCounts = new int[usersAtDate.size()];
				int i = 0;
				for (String user : usersAtDate) {
					requestCounts[i++] = dateUserRequestNumberMap.get(date)
							.get(user);
				}
				Arrays.sort(requestCounts);
				int lowerIndex = (int) (usersAtDate.size() * DataSetHeader.DROP_USER_THRESHOLD);
				int upperIndex = (int) (usersAtDate.size() * (1 - DataSetHeader.DROP_USER_THRESHOLD));
				int lowerCount = requestCounts[lowerIndex];
				int upperCount = requestCounts[upperIndex];
				k.set(date);
				v.set(lowerCount + " " + upperCount);
				m.write(k, v, DataSetHeader.REQUEST_RANGE_INDEX_DIRECTORY
						+ "/header");
				/*
				 * LOG.info("Request Range for " + date + ": " + lowerCount +
				 * " " + upperCount);
				 */

				k.set(date);
				v.set("" + usersAtDate.size());
				m.write(k, v, DataSetHeader.NUM_INSTANCES_DIRECTORY + "/header");
				LOG.info("Number of instances for " + date + ": " + v);
			}
			/*
			 * m.close(); fos.close();
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}

		LOG.info("CLEANUP; Writing Occurence Matrix");
		try {
			String[] sortedUsers = usersDateMap.keySet().toArray(
					new String[usersDateMap.size()]);
			Arrays.sort(sortedUsers);
			for (String user : sortedUsers) {
				Set<String> datesOfUsers = usersDateMap.get(user).keySet();

				String datesString = Util.joinArrayToString(
						new ArrayList<String>(datesOfUsers), ",");

				k.set(user);
				v.set(datesString);
				m.write(k, v, DataSetHeader.OCCURRENCE_MATRIX_DIRECTORY
						+ "/header");
			}

			// close the multiple output
			m.close();
			fos.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {

		// sum up all entries
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
			Util.ping(context, GenerateDataSetHeaderReducer.class);
		}

		// split the key. the first index holds the type of index, the second
		// one the name of the index
		String[] rr = key.toString().split(" ");

		// increase the counter
		if (rr[0].equals(DataSetHeader.HOST_INDEX_DIRECTORY)) {

			long indexOfCurrentHostname = context.getCounter(
					GenerateDataSetHeaderTool.CounterTypes.NUM_ATTRIBUTES)
					.getValue();

			context.getCounter(
					GenerateDataSetHeaderTool.CounterTypes.NUM_ATTRIBUTES)
					.increment(1);

			if (WRITE_HOST_INDEX_MAPPING) {
				k.set(Long.toString(indexOfCurrentHostname));
				v.set(rr[2]);
				m.write(k, v, DataSetHeader.HOST_INDEX_MAPPING_DIRECTORY);
			}

			fos.write(Util.hexStringToByteArray(rr[1]));
		} else if (rr[0].equals(DataSetHeader.USER_INDEX_DIRECTORY)) {
			context.getCounter(
					GenerateDataSetHeaderTool.CounterTypes.NUM_CLASSES)
					.increment(1);
			k.set(rr[1]);
			v.set(Long.toString(sum));
			m.write(k, v, rr[0] + "/header");
		} else if (rr[0].equals(DataSetHeader.REQUEST_RANGE_INDEX_DIRECTORY)) {
			// rr[1]: user@date
			String[] rrr = rr[1].split("@");

			HashMap<String, Integer> userRequestNumberMap = dateUserRequestNumberMap
					.get(rrr[1]);
			if (userRequestNumberMap == null) {
				userRequestNumberMap = new HashMap<String, Integer>();
				dateUserRequestNumberMap.put(rrr[1], userRequestNumberMap);
			}
			Integer count = userRequestNumberMap.get(rrr[0]);
			int countAsInt = 0;
			if (count != null) {
				countAsInt = count.intValue();
			}
			countAsInt += sum;
			userRequestNumberMap.put(rrr[0], countAsInt);

			// nun die occurrence matrix hashmap bef�llen
			HashMap<String, Integer> datesOfUser = usersDateMap.get(rrr[0]);
			if (datesOfUser == null) {
				datesOfUser = new HashMap<String, Integer>();
				usersDateMap.put(rrr[0], datesOfUser);
			}
			datesOfUser.put(rrr[1], 1); // remember that user was active at this
			// date
		}
	}

	@Override
	public void setup(Context context) throws IOException {
		// initialize MultipleOutputs
		m = new MultipleOutputs<Text, Text>(context);
		FileSystem fs = FileSystem.get(context.getConfiguration());

		WRITE_HOST_INDEX_MAPPING = context.getConfiguration().getBoolean(
				Util.CONF_WRITE_HOST_INDEX_MAPPING, false);

		fos = fs.create(new Path(context.getConfiguration().get(
				Util.CONF_HEADER_PATH)
				+ "/" + DataSetHeader.HOST_INDEX_DIRECTORY));

	}
}
