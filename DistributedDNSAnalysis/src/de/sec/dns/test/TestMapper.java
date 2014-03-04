package de.sec.dns.test;

import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongShortHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.dataset.Instance;
import de.sec.dns.util.ClassIndex;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * The mapper class for {@link TestTool}. It calculates the probabilty of each
 * class for a given instance.
 * 
 * @author Christian Banse
 */
public class TestMapper extends
		Mapper<LongWritable, ObjectWritable, Text, DoubleArrayWritable> {

	/**
	 * Counts the mapping entries. Used for the performance evaluation.
	 */
	private static int counter = 0;

	/**
	 * Holds the last time the map function was called. Used for the performance
	 * evaluation.
	 */
	private static long lastTime;

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(TestMapper.class);

	/**
	 * Holds the total time the job has been mapping. Used for the performance
	 * evaluation.
	 */
	private static long totalTime = 0;

	/**
	 * A temporary variable for the value of the mapreduce entry.
	 */
	private DoubleArrayWritable array = new DoubleArrayWritable();

	/**
	 * The class index.
	 */
	private ClassIndex classIndex;

	/**
	 * The current <i>Context</i>
	 */
	private Context context;

	/**
	 * This hashmap contains all hosts that meet the minimum DF criterion. Only
	 * hosts included in this map are included.
	 * 
	 * If this map is null, all hosts will be included regardless of their DF.
	 */
	private TLongFloatHashMap hostsWithMinimumDF = null;

	/**
	 * The probabilities of a word given class for the specified day.
	 */
	private HashMap<String, TrainingData> probWordGivenClass = new HashMap<String, TrainingData>();

	/**
	 * is set by setup method in case of fatal failure
	 */
	private Exception setupFailedException;

	/**
	 * if at least this time is needed for mapping a single instance it will be
	 * reported
	 */
	private long REPORT_SLOW_INSTANCE_THRESHOLD = 60;

	public static char NO_INSTANCE = '-';

	/**
	 * Calculates the logarithmic probability of a class that is missing on the
	 * training day.
	 * 
	 * @param instance
	 *            The instance that should be tested.
	 * @return The logarithmic probability.
	 */
	private double getLogOfMissingClass(Instance instance) {
		double answer = 0.0d;

		int numAttributes = context.getConfiguration().getInt(
				Util.CONF_NUM_ATTRIBUTES, 1);

		// calculate the probability of an attribute that
		// does not exist within an instance that does not exist
		double laplaceValue = Util.bayes(0, 0, numAttributes);

		// loop through all values of the instance
		for (int i = 0; i < instance.getNumValues(); i++) {
			answer += (instance.getValue(i) * laplaceValue);
		}

		return answer;
	}

	private double cosim(Instance instance, String classIndex) {
		// retrieve the occurence map for the given class
		TrainingData td = probWordGivenClass.get(classIndex);

		TLongShortHashMap map = td.map;

		int wordsPerClass = map.size() - 1;

		int numAttributes = context.getConfiguration().getInt(
				Util.CONF_NUM_ATTRIBUTES, 1);

		short indexInLUT;
		float w1;
		float w2;

		double dotproduct = 0;

		for (int i = 0; i < instance.getNumValues(); i++) {
			w1 = (float) instance.getValue(i);
			long attributeIndex = instance.getIndex(i);

			indexInLUT = map.get(attributeIndex);
			float d = td.valueLUT[indexInLUT];

			if (map.get(attributeIndex) != 0) {
				w2 = Util.reverseBayes(d, wordsPerClass, numAttributes);
			} else {
				w2 = 0;
			}

			dotproduct += (w1 * w2);
		}

		double magnitude1 = 0;

		for (int i = 0; i < instance.getNumValues(); i++) {
			w1 = (float) instance.getValue(i);
			magnitude1 += w1 * w1;
		}

		magnitude1 = Math.sqrt(magnitude1);

		double magnitude2 = 0;
		long[] attributes = map.keys();

		for (int i = 0; i < attributes.length; i++) {
			if (attributes[i] == -1) {
				continue;
			}
			indexInLUT = map.get(attributes[i]);
			float d = td.valueLUT[indexInLUT];

			w2 = Util.reverseBayes(d, wordsPerClass, numAttributes);
			magnitude2 += w2 * w2;
		}

		magnitude2 = Math.sqrt(magnitude2);

		return dotproduct / (magnitude1 * magnitude2);
	}

	/**
	 * Calculates the logarithmic probability for an instance given a specified
	 * class.
	 * 
	 * @param instance
	 *            The instance that will be tested.
	 * @param classIndex
	 *            The class it will be tested against.
	 * @return The logarithmic probability.
	 * @throws Exception
	 *             if an error has occured.
	 */
	private double logOfInstanceGivenClass(Instance instance, String classIndex)
			throws Exception {
		double answer = 0;
		double freqOfWordInDoc;

		// retrieve the occurence map for the given class
		TrainingData td = probWordGivenClass.get(classIndex);

		TLongShortHashMap map = td.map;

		// cache the laplaceValue in case we need it
		if (map.get(Util.MISSING_ATTRIBUTE_INDEX) == 0) {
			LOG.error("MISSING_ATTRIBUTE_INDEX is not contained in map for class "
					+ classIndex);
			throw new Exception(
					"MISSING_ATTRIBUTE_INDEX is not contained in map for class "
							+ classIndex);
		}
		short indexInLUT = map.get(Util.MISSING_ATTRIBUTE_INDEX);
		// Byte: 126, 127, -128, -127, ...=> 126, 127, 128, 129, ...
		// damit man negative Bytes elegant in positive wandeln kann machen wir
		// + 256 % 256:
		float laplaceValue = td.valueLUT[indexInLUT];

		// loop through all values of the instance
		for (int i = 0; i < instance.getNumValues(); i++) {
			freqOfWordInDoc = instance.getValue(i);
			long attributeIndex = instance.getIndex(i);

			// retrieve the stored probability for the attribute in the training
			// data
			// Beware: map.get(attributeIndex) == 0 --> missing key/value!
			indexInLUT = map.get(attributeIndex);
			float d = td.valueLUT[indexInLUT];

			// wrong:
			// if ((int)d != 0) {

			// correct:
			if (map.get(attributeIndex) != 0) {
				answer += (freqOfWordInDoc * d);
			} else {
				// if the attribute is not found in the training data, use the
				// laplaceValue
				answer += (freqOfWordInDoc * laplaceValue);
			}

			Util.ping(context, TestMapper.class);
		}

		return answer;
	}

	@Override
	public void map(LongWritable key, ObjectWritable obj, Context context)
			throws IOException, InterruptedException {

		if (setupFailedException != null) {
			throw new IOException(setupFailedException);
		}

		DoubleWritable logDocGivenClass = new DoubleWritable();
		DoubleWritable classI = new DoubleWritable();
		DoubleWritable cosim = new DoubleWritable();

		lastTime = System.currentTimeMillis();

		Instance instance;

		instance = (Instance) obj.get();

		// remove all hosts whose DF is below the threshold
		if (hostsWithMinimumDF != null) {
			instance.setAcceptableIndices(hostsWithMinimumDF.keySet());
		}

		// loop through all known classes
		for (int i = 0; i < instance.getNumClasses(); i++) {

			boolean skipThisClass = false;

			try {
				// check if the class of the test instance was present on the
				// training day
				String classLabel = classIndex.getClassname(i);

				cosim.set(0);
				if (!probWordGivenClass.containsKey(classLabel)) {
					// class of test instance was not found on the training day
					// der alte Code hier ist vermutlich fehlerhaft bzw.
					// unlogisch
					// wieso wird die Instanz hier mit einer Klasse verglichen,
					// die es am Trainingstag gar nicht gibt?
					// testweise machen wir das jetzt einmal NICHT und
					// �berspringen solche Klassen.
					// Als der alte Code noch verwendet wurde, kam es n�mlich
					// (f�lschlicherweise) vor, dass
					// Instanzen einer Klasse zugewiesen wurden, f�r die es am
					// Vortag gar keine Instanz gab.
					// logDocGivenClass.set(getLogOfMissingClass(instance));
					skipThisClass = true;
				} else {
					// retrieve the probability of the given class
					logDocGivenClass.set(logOfInstanceGivenClass(instance,
							classLabel));

					if (Util.isEnabled(context.getConfiguration(),
							Util.CONF_TEST_DROP_AMBIGUOUS_RESULTS)) {
						cosim.set(cosim(instance, classLabel));
					}
				}

				// wenn die Klase am Trainingstag nicht vorkam, �berspringen
				// wir
				// sie
				if (skipThisClass)
					continue;

				classI.set(i);

				// store it in an array with the classIndex
				array.set(new DoubleWritable[] { logDocGivenClass, classI,
						cosim });

				// and hand it to the reducer
				context.write(new Text(instance.getId()), array);
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("map failed with exception");
				throw new IOException(e);
			}
		}

		// count the number of instances per class
		// context.write(new Text(Util.INSTANCES_PER_CLASS_PATH + " " +
		// instance.getClassLabel()), ONE);

		counter++;

		long timeTaken = System.currentTimeMillis() - lastTime;
		totalTime += timeTaken;

		if ((counter % 10) == 0) {
			// print out some performance stuff
			LOG.info("instance " + counter + " duration: "
					+ ((double) timeTaken / 1000) + " s - avg : "
					+ ((double) (totalTime / counter) / 1000) + " s"
					+ " num_values: " + instance.getNumValues());
		}

		double duration = ((double) timeTaken / 1000);
		if (duration > REPORT_SLOW_INSTANCE_THRESHOLD) {
			LOG.info("Mapped a particularly SLOW INSTANCE. classLabel: "
					+ instance.getClassLabel() + ", " + "duration: " + duration
					+ " s (" + duration / 60 + " min)," + " num_values: "
					+ instance.getNumValues());
		}

		//
	}

	public void readProbabiltyOfWordGivenClass() throws IOException,
			ClassNotFoundException {
		String line = null;
		String rr[] = null;
		// TIntByteHashMap map = null;
		Path file = null;

		Configuration conf = context.getConfiguration();

		Path trainingPath = new Path(conf.get(Util.CONF_TRAINING_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/"
				+ conf.get(Util.CONF_OPTIONS) + "/"
				+ Util.PROBABILITY_OF_WORD_GIVEN_CLASS_PATH + "/"
				+ conf.get(Util.CONF_TRAINING_DATE));
		FileSystem fs = FileSystem.get(conf);

		TrainingData td = null;
		String lastClass = null;
		StringBuilder sb = new StringBuilder();
		for (FileStatus fileStatus : fs.listStatus(trainingPath)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();

			LOG.info("reading from " + file + "...");

			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			int i = 0;
			while (true) {
				line = reader.readLine();

				if (i++ % 10000 == 0) {
					Util.ping(context, TestMapper.class);
					i = 0;
				}

				if (line == null) {
					break;
				}

				rr = Util.veryFastSplit(line, '\t', 3);

				// rr[0] class
				// rr[1] attributeIndex
				// rr[2] value

				// braucht weniger RAM
				// damit der lange String "line" nicht aufbewahrt wird
				sb.setLength(0); // empty out the sb
				sb.append(line.substring(0, line.indexOf("\t"))); // = rr[0]

				line = null;
				String classLabel = sb.toString();
				int attributeIndex = Integer.parseInt(rr[1]);

				if ((td == null) || !lastClass.equals(classLabel)) {
					// if(td!=null)
					// LOG.info("Loaded data for class "+lastClass+": "+td.map.size()+" hosts; "+td.nextFreeSlot+" values");
					td = new TrainingData();
					probWordGivenClass.put(classLabel, td);
					lastClass = classLabel;
				}
				/*
				 * unn�tig: } else { td = trainingData.get(classLabel); }
				 */

				float value = Float.parseFloat(rr[2]);
				rr = null;

				Integer indexInLUT = null;
				for (int j = 1; j < td.valueLUT.length; j++) {
					if (td.valueLUT[j] == value) {
						indexInLUT = j;
						// LOG.info("Found entry "+value+" in LUT: "+j);
						break;
					}
				}
				if (indexInLUT == null) {
					if (td.nextFreeSlot >= td.valueLUT.length) {

						if (td.valueLUT.length <= Short.MAX_VALUE) { // enlarge
																		// from
																		// 64
																		// LOG.info("Byte Lookup Table index overflow for class "+classLabel);

							float[] valueLUT = new float[2 * td.valueLUT.length];

							System.arraycopy(td.valueLUT, 0, valueLUT, 0,
									td.valueLUT.length);

							td.valueLUT = valueLUT;

						} else {
							LOG.error("Fatal Byte Lookup Table index overflow (>"
									+ Short.MAX_VALUE
									+ ") for class "
									+ classLabel);

							reader.close();
							throw new IOException(
									"Fatal Byte Lookup Table index overflow (>"
											+ Short.MAX_VALUE + ") for class "
											+ classLabel);
						}
					}
					td.valueLUT[td.nextFreeSlot] = value;
					indexInLUT = (int) td.nextFreeSlot++;
				}
				// Byte: 126, 127, -128, -127, ...=> 126, 127, 128, 129, ...
				td.map.put(attributeIndex, indexInLUT.shortValue());
			}

			Util.getMemoryInfo(TestMapper.class);
		}

		line = null;
		rr = null;

		LOG.info("training day has " + probWordGivenClass.size()
				+ " classes/instances");
		Util.getMemoryInfo(TestMapper.class);
	}

	@Override
	public void setup(Context context) {

		this.context = context;

		classIndex = ClassIndex.getInstance();

		Configuration conf = context.getConfiguration();

		float threshold = conf.getFloat(Util.CONF_MINIMUM_DF_OF_HOSTS, 0);

		try {
			LOG.info("loading training data...");

			readProbabiltyOfWordGivenClass();

			if (threshold > 0.0) {
				LOG.info("loading DF values");
				hostsWithMinimumDF = Util.getHostsWithDocumentFrequencies(conf,
						threshold);
			}

			if (!classIndex.isPopulated()) {
				classIndex.init(conf);
				classIndex.populateIndex();
			}
		} catch (Exception e) {
			LOG.error("setup failed with an exception!");
			e.printStackTrace();
			setupFailedException = e;
		}
	}
}

class TrainingData {
	public TLongShortHashMap map = new TLongShortHashMap();
	short nextFreeSlot = 1; // do not start with 0 because 0 is the
	// missing-value-Value of the T-Hashmap!
	public float[] valueLUT = new float[64];
}