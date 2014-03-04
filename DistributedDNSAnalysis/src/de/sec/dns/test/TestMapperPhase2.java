package de.sec.dns.test;

import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongShortHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

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
public class TestMapperPhase2 extends
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
	private static final Log LOG = LogFactory.getLog(TestMapperPhase2.class);

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
	 * The confusion matrix that resulted from the first pass. Used to determine
	 * already assigned instances that will be skipped in the second pass. The
	 * second pass works on unassigned instances/classes only.
	 */
	private ConfusionMatrix firstPassConfusionMatrix = null;

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

			Util.ping(context, TestMapperPhase2.class);
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

				if (previousPassInstanceClassMap.containsKey(instance.getId())) {
					if (previousPassInstanceClassMap.get(instance.getId())
							.equals(classLabel)) {
						// d.h. die vorliegende Instanz wurde beim vorherigen
						// Durchlauf genau der aktuellen Klasse zugewiesen
						;
						// dann soll der Wert auch wieder zugewiesen werden,
						// damit sie auch im aktuellen Durchlauf wieder
						// dieser Klasse zugewiesen wird.
					} else {
						// die vorliegende Instanz einer anderen als der
						// aktuellen Klasse zugewiesen
						// dann darf sie der aktuellen Klasse auf keinen Fall
						// mehr zugewiesen werden
						continue;
					}
				} else if (previousPassClassInstanceMap.containsKey(classLabel)) {
					// d.h. dieser Klasse wurde beim vorherigen Durchlauf eine
					// andere Instanz zugewiesen
					continue;
					// dann soll dieser Klasse auf keinen Fall diese Instanz
					// zugeordnet werden.
				} else {
					// d.h. die vorliegende Instanz wurde beim vorherigen
					// Durchlauf keiner Klasse zugeordnet
					// UND der aktuelle Klasse wurde beim vorherigen keine
					// Instanz zugewiesen
					;
					// in diesem Fall k�nnen wir also noch einen Match
					// herbeif�hren
				}

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
					Util.ping(context, TestMapperPhase2.class);
					i = 0;
				}

				if (line == null) {
					break;
				}

				rr = line.split("\t");

				// rr[0] class
				// rr[1] attributeIndex
				// rr[2] value

				String classLabel = rr[0];
				int attributeIndex = Integer.parseInt(rr[1]);
				float value;

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

				value = Float.parseFloat(rr[2]);

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
		}

		line = null;
		rr = null;

		LOG.info("training day has " + probWordGivenClass.size()
				+ " classes/instances");
		Util.getMemoryInfo(TestMapperPhase2.class);
	}

	// public final String previousPassMapString =
	// "106_1;1129_1,1|106d_2;451_1,1|109_1;109_1,1|1166_2;7dc_1,1|117_1;1277_1,1|11ab_2;b01_2,1|11d6_1;1683_2,1|11e_1;528_2,1|126_1;198_1,1|1277_1;ade_1,1|138_1;2ae_2,1|151_1;151_1,1|152d_2;2f0_1,1|1550_1;d5_1,1|164b_1;5ed_1,1|167a_2;f20_2,1|1823_1;656_1,1|186_2;1475_1,1|18e_1;18e_1,1|1a4_1;aeb_2,1|1c1_1;1c1_1,1|1d52_1;d90_2,1|1ec6_1;aa_1,1|1ec_1;a96_1,1|1ede_1;358_2,1|1fc_1;299_2,1|204_1;204_1,1|20b_1;b8b_1,1|211_1;40a_1,1|216_1;37f_2,1|232_1;b13_1,1|247_1;247_1,1|265_2;18d_1,1|289_1;f16_1,1|28a_1;28a_1,1|2a0_1;4c1_2,1|2ab_1;32f_1,1|2ae_2;4a3_2,1|32f_1;c92_1,1|332_1;124_1,1|341_2;5da_1,1|344_2;344_2,1|370_1;370_1,1|379_1;12d_2,1|386_1;11a_1,1|3a4_1;3a4_1,1|3ba_1;2a2_1,1|3c_1;3c_1,1|3d_1;295_1,1|3e1_1;1fc_1,1|40c_1;8b_1,1|44_1;44_1,1|482_2;1c9_1,1|492_1;492_1,1|496_1;210_2,1|4a4_1;3d_1,1|4a6_2;a27_1,1|4aa_1;c76_1,1|4b3_1;192_1,1|4c6_1;4c6_1,1|4cd_1;1db_2,1|500_1;1a0_1,1|512_2;b59_1,1|51_1;28c_2,1|527_2;4a6_2,1|537_1;537_1,1|55_1;8a7_1,1|584_1;312_1,1|599_1;541_2,1|5a3_1;229_1,1|5d5_1;2c9_1,1|5da_1;4f5_1,1|5e_1;7eb_1,1|5ed_1;39_1,1|5f7_2;697_1,1|5f8_2;5be_1,1|675_1;675_1,1|68_1;68_1,1|68a_1;eae_1,1|6c8_1;afc_2,1|6f_1;21f_1,1|72e_2;516_1,1|759_1;34_1,1|76_1;877_1,1|79a_1;360_2,1|7a_2;5c6_2,1|7ac_1;54a_1,1|7cf_1;6b3_1,1|7d_1;6d8_1,1|7e1_1;73_1,1|820_1;11fd_2,1|832_1;73f_1,1|92e_2;2c8_1,1|932_1;2d7_1,1|95_1;30e_1,1|96a_2;102_2,1|99a_1;77c_1,1|9ef_1;9ef_1,1|a02_1;1f7_1,1|a0f_1;4e_1,1|a27_1;12bf_1,1|a54_1;341_2,1|a84_2;699_2,1|a9a_1;a9a_1,1|a9e_2;9be_1,1|ace_1;187_1,1|b4_1;95_1,1|b65_1;b65_1,1|b8e_1;b8e_1,1|b9d_1;277_1,1|c5_1;79_1,1|c92_1;8db_1,1|cd_1;36d_1,1|d68_2;62e_1,1|d6d_1;2dd_1,1|e82_1;32_1,1|e9_1;406_1,1|ea5_1;7d_1,1|f92_2;2f1_1,1|fc4_1;417_1,1";
	private Map<String, String> previousPassClassInstanceMap = new HashMap<String, String>();
	private Map<String, String> previousPassInstanceClassMap = new HashMap<String, String>();

	@Override
	public void setup(Context context) {

		this.context = context;

		classIndex = ClassIndex.getInstance();

		Configuration conf = context.getConfiguration();

		float threshold = conf.getFloat(Util.CONF_MINIMUM_DF_OF_HOSTS, 0);

		// previous pass map f�llen
		/*
		 * String[] tuples = Util.fastSplit(previousPassMapString, '|');
		 * for(String tuple : tuples) { String[] pair =
		 * Util.veryFastSplit(tuple, ';', 2);
		 * previousPassClassInstanceMap.put(pair[0], pair[1]);
		 * previousPassInstanceClassMap.put(pair[1], pair[0]); }
		 */

		try {

			LOG.info("Reading confusion matrix from previous pass...");
			String matrixPath = conf.get(Util.CONF_MATRIX_PATH) + "/"
					+ conf.get(Util.CONF_SESSION_DURATION) + "/"
					+ conf.get(Util.CONF_OPTIONS) + "/"
					+ conf.get(Util.CONF_TEST_DATE) + "/part-r-00000";

			firstPassConfusionMatrix = new ConfusionMatrix(
					FileSystem.get(conf), matrixPath);
			for (String classLabel : firstPassConfusionMatrix.keySet()) {
				if (classLabel.equals("UNKNOWN"))
					continue;
				String[] instances = firstPassConfusionMatrix.get(classLabel)
						.getClassifiedInstances();
				if (instances.length > 0) {
					previousPassClassInstanceMap.put(classLabel, instances[0]);
					previousPassInstanceClassMap.put(instances[0], classLabel);
				}
			}

			LOG.info("Read " + previousPassInstanceClassMap.size()
					+ " mappings from confusion matrix");

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
