package de.sec.dns.training;

import gnu.trove.map.hash.TLongFloatHashMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import de.sec.dns.dataset.Instance;
import de.sec.dns.util.ClassIndex;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * Training mapper for {@link YangTrainingTool}. It hands an tuple consisting of
 * the attribute index and the occurrence of the attribute to the reducer.
 * 
 * @author Elmo Randschau
 */
public class YangTrainingMapper extends
		Mapper<LongWritable, ObjectWritable, Text, DoubleArrayWritable> {
	static int instances = 0;

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(YangTrainingMapper.class);

	private static boolean TRAIN_WITH_LIMITED_DATASET;

	private DoubleWritable one = new DoubleWritable(1);

	/**
	 * This hashmap contains all hosts that meet the minimum DF criterion. Only
	 * hosts included in this map are included.
	 * 
	 * If this map is null, all hosts will be included regardless of their DF.
	 */
	private TLongFloatHashMap hostsWithMinimumDF = null;

	/**
	 * A temporary variable used as mapreduce key.
	 */
	private Text k = new Text();

	private DoubleWritable[] ONE_ARRAY = new DoubleWritable[] { new DoubleWritable(
			1) };

	private String[] oneTenthOfClassIndex = null;

	/**
	 * is set by setup method in case of fatal failure
	 */
	private Exception setupFailedException;

	/**
	 * A temporary variable used as mapreduce value.
	 */
	private DoubleArrayWritable v = new DoubleArrayWritable();

	Calendar firstSession;
	Calendar lastSession;

	int sessionDuration;

	@Override
	public void map(LongWritable key, ObjectWritable obj, Context context)
			throws IOException, InterruptedException {
		Instance instance;
		String classLabel;

		if (setupFailedException != null) {
			throw new IOException(setupFailedException);
		}

		FileSplit file = (FileSplit) context.getInputSplit();

		// Extract date from path
		String date = file.getPath().getParent().getName();

		instance = (Instance) obj.get();

		if (TRAIN_WITH_LIMITED_DATASET) {
			// wir trainieren nur mit 1/10, der rest kommt in die UNKNOWN-Klasse
			// bei NUM_OF_REDUCE_TASKS darf 1
			if (Arrays.binarySearch(oneTenthOfClassIndex,
					instance.getClassLabel()) < 0) {
				classLabel = "UNKNOWN";
			} else {
				classLabel = instance.getClassLabel();
			}
		} else {
			classLabel = instance.getClassLabel();
		}

		// additional training instances, so we need to add them to all
		// other training days
		if (date.equals("additionalTrainingInstances")) {
			Calendar session = (Calendar) firstSession.clone();

			while (session.before(lastSession)) {
				date = Util.formatDate(session.getTime());

				writeInstance(context, instance, classLabel, date);

				session.add(Calendar.MINUTE, sessionDuration);
			}

			// last day, too
			date = Util.formatDate(session.getTime());
		}

		writeInstance(context, instance, classLabel, date);

		v.set(ONE_ARRAY);

		if (date.equals("additionalTrainingInstances")) {
			// additional training instances, so we need to add them to all
			// other training days
			Calendar session = (Calendar) firstSession.clone();

			while (session.before(lastSession)) {
				date = Util.formatDate(session.getTime());
				k.set(Util.PROBABILITY_OF_CLASS_PATH + " " + date + " "
						+ classLabel);
				context.write(k, v);

				session.add(Calendar.MINUTE, sessionDuration);
			}

			// last day, too
			date = Util.formatDate(session.getTime());
		}

		k.set(Util.PROBABILITY_OF_CLASS_PATH + " " + date + " " + classLabel);
		context.write(k, v);

		instances++;
	}

	private void writeInstance(Context context, Instance instance,
			String classLabel, String date) throws IOException,
			InterruptedException {
		double numOccurences;
		k.set("profiling" + " " + date + " " + classLabel);
		for (int a = 0; a < instance.getNumValues(); a++) {
			numOccurences = instance.getValue(a);
			if (numOccurences < 0) {
				throw new IOException(
						"Numeric attribute values must all be greater or equal to zero.");
			}

			// key = profiling date classLabel
			// value = {hostIndex,numOccurence}

			long indexOfHost = instance.getIndex(a);
			if ((hostsWithMinimumDF == null)
					|| ((hostsWithMinimumDF != null) && hostsWithMinimumDF
							.contains(indexOfHost))) {
				// v.set(new DoubleWritable[] { new DoubleWritable(indexOfHost),
				// new
				// DoubleWritable(numOccurences) });

				// the actual occurrence is not needed, instead we save a 'ONE'
				// entry to signal the
				// reducer this Host occurred in this instance
				v.set(new DoubleWritable[] { new DoubleWritable(indexOfHost),
						one });

				context.write(k, v);
			}
		}
		// write the Session Count, so the Reducer will know how many Instances
		// the Class has.
		v.set(new DoubleWritable[] { new DoubleWritable(Util.SESSION_COUNT),
				one });
		context.write(k, v);
	}

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();

		try {
			firstSession = Util.getCalendarFromString(conf
					.get(Util.CONF_FIRST_SESSION));
			lastSession = Util.getCalendarFromString(conf
					.get(Util.CONF_LAST_SESSION));

			sessionDuration = Util.getInt(conf, Util.CONF_SESSION_DURATION);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		TRAIN_WITH_LIMITED_DATASET = conf.getBoolean(
				Util.CONF_TRAINING_WITH_LIMITED_DATASET, false);

		if (TRAIN_WITH_LIMITED_DATASET) {
			ClassIndex classIndex = ClassIndex.getInstance();

			try {
				if (!classIndex.isPopulated()) {
					classIndex.init(conf);
					classIndex.populateIndex();
				}

				int oneTenthOfClassIndexSize = classIndex.getSize()
						/ conf.getInt(Util.CONF_TRAINING_USER_RATIO, 10);
				oneTenthOfClassIndex = new String[oneTenthOfClassIndexSize];
				int[] chosenIndices = new int[oneTenthOfClassIndexSize];

				Random random = new Random();
				random.setSeed(conf.getInt(Util.CONF_TRAINING_SEED, 0));

				int i = 0;
				do {
					int index = random.nextInt(classIndex.getSize());
					if (Util.isInArray(chosenIndices, index)) {
						continue;
					}

					chosenIndices[i] = index;
					oneTenthOfClassIndex[i] = classIndex.getClassname(index);
					i++;
				} while (i < oneTenthOfClassIndexSize);
				Arrays.sort(oneTenthOfClassIndex);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		float threshold = conf.getFloat(Util.CONF_MINIMUM_DF_OF_HOSTS, 0);

		if (threshold > 0.0) {
			try {
				hostsWithMinimumDF = Util.getHostsWithDocumentFrequencies(conf,
						threshold);
			} catch (Exception e) {
				LOG.error("setup failed with an exception!");
				e.printStackTrace();
				setupFailedException = e;
			}
		}
	}
}
