package de.sec.dns.training;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.test.YangTestMapper;
import de.sec.dns.test.YangTestReducer;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * The reducer class for {@link YangTrainingTool}. It generates the lift or
 * support based profiles
 * 
 * @author Elmo Randschau
 */
public class YangTrainingReducer extends
		Reducer<Text, DoubleArrayWritable, Text, Writable> {

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(YangTrainingReducer.class);

	// default settings...
	private static boolean SKIP_SUPPORT = false;
	private static boolean SKIP_LIFT = true;

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<Text, Text> m;

	private DoubleWritable zero = new DoubleWritable(0);

	/**
	 * a Map containing the Candidate-Patterns of a date
	 */
	private HashMap<String, TIntArrayList> _candidateMap;

	/**
	 * Map containing host indices and their idf for the whole dataset. The
	 * object is initialized in setup() once the number of hosts is known hostID
	 * <-> occurence
	 */
	private TIntFloatHashMap _dailyDocFreqMap;

	private String dateOfDdfs;
	// remembers the date of the ddfs so we can avoid re-reading them
	// unnecessarily

	/**
	 * Map containing the number of instances (users) for a Date
	 */
	private HashMap<String, Integer> numInstances = new HashMap<String, Integer>();

	/**
	 * A temporary variable for the mapreduce value.
	 */
	private Text v = new Text();

	/**
	 * just a counter, for counting the number of reduced Instances
	 */
	private static int counter = 0;

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
	protected void reduce(Text dateAndClassLabel,
			Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		/**
		 * map containing HostID - occurrence
		 */
		TIntDoubleHashMap map = new TIntDoubleHashMap();

		/**
		 * the number of Instances from this User on the given Date
		 */
		double numSession = 0.0;

		String[] str = dateAndClassLabel.toString().split(" ");
		String prob = str[0];

		if (prob.equals("profiling")) {
			String trainingdate = str[1];
			String classLabel = str[2];

			readDailyDocumentFrequencies(context, trainingdate);

			// if ((counter++ % 10) == 0)
			// {
			// LOG.info("reducing instance " + classLabel);
			// }

			// loop through all values, put them into a hashmap
			// generates a map containing all Hosts for one User and one Session
			int valueCounter = 0;
			for (DoubleArrayWritable rr : values) {
				Writable[] w = rr.get();
				// w[0] = attributeIndex
				// w[1] = numOccurences in RAW format
				double occ = ((DoubleWritable) w[1]).get();

				int attributeIndex = (int) ((DoubleWritable) w[0]).get();

				// if the attributeIndex is the SESSION_COUNT marker count up
				// the number of Instences
				// from this User
				if (attributeIndex == Util.SESSION_COUNT) {
					numSession = numSession + 1.0;
					continue;
				}

				double value = map.get(attributeIndex);
				map.put(attributeIndex, occ + value);

				Util.ping(context, YangTrainingReducer.class);

				valueCounter++;
				if (valueCounter % 10000 == 0)
					Util.getMemoryInfo(YangTrainingReducer.class);
			}
			LOG.info("Class: " + classLabel + " has: " + numSession
					+ " Instances");
			map.put(Util.MISSING_ATTRIBUTE_INDEX, 0.0);

			// generate Profile Data Vector
			DoubleWritable[] withinUserStrength = new DoubleWritable[_candidateMap
					.get(trainingdate).size()];

			// create a StringBuilder for Support Profile Output
			StringBuilder sb = new StringBuilder();
			sb.append(_candidateMap.get(trainingdate).size());
			sb.append("\t").append("{ ");

			// calculate the WithinUserStrength
			// loop through all CandidatePattern, we can not loop through the
			// profile because it is
			// sparse
			for (int i = 0; i < _candidateMap.get(trainingdate).size(); i++) {
				// we only want to save non 0.0 Profile Data...
				if (map.containsKey(_candidateMap.get(trainingdate).get(i))) {
					double tmp = map
							.get(_candidateMap.get(trainingdate).get(i))
							/ numSession;

					tmp = (double) ((int) (tmp * 1000000)) / 1000000;

					// add the entry to the withinUserStrength
					withinUserStrength[i] = new DoubleWritable(tmp);
					sb.append(_candidateMap.get(trainingdate).get(i)).append(
							" ");
					sb.append(tmp).append(" ");
				}
				// for Lift Profiling we still need the complete
				// withinUSerStrength Array including zero
				// values
				else {
					withinUserStrength[i] = zero;
				}
				// Util.ping(context, TrainingYangReducer.class);
			}

			// write the support output
			if (!SKIP_SUPPORT) {
				Text k = new Text();
				k.set(classLabel + "\t" + "sp");
				sb.append("}");
				v.set(sb.toString());
				m.write(k, v, "support" + "/" + trainingdate + "/");
			}

			// calculate / retrieve the overall pattern strength
			if (!SKIP_LIFT) {
				// create a new StringBuilder
				sb = new StringBuilder();
				// append CandidatePattern size
				sb.append(_candidateMap.get(trainingdate).size());
				sb.append("\t").append("{ ");

				// the overallPatternStrength represents the relative
				// probability of a Pattern in the
				// Sessions from all Users
				Double[] overallPatternStrength = new Double[_candidateMap.get(
						trainingdate).size()];

				// total number of Sessions on the Traningdate
				double D = (double) numInstances.get(trainingdate);

				// loop through all CandidatePatterns
				for (int i = 0; i < _candidateMap.get(trainingdate).size(); i++) {
					// only if the User requested the Candidate-Pattern add it
					// to the Profile
					if (!map.containsKey(_candidateMap.get(trainingdate).get(i))) {
						continue;
					}

					// retrieve occurrence of this CandidatePattern for all
					// Users on the TrainingDate
					double Dpj = (double) _dailyDocFreqMap.get(_candidateMap
							.get(trainingdate).get(i));

					// set the Overall Pattern Strength for this
					// CandidatePattern
					overallPatternStrength[i] = new Double(Dpj / D);

					// some extra calculation steps for debugging
					double t1 = withinUserStrength[i].get();
					double t2 = overallPatternStrength[i];
					double profileData = t1 / t2;
					// profileData = (profileData >= 0.0) ? profileData : 0.0;

					profileData = (double) ((int) (profileData * 1000000)) / 1000000;
					// if
					// (map.containsKey(_candidateMap.get(trainingdate).get(i)))
					{
						sb.append(_candidateMap.get(trainingdate).get(i))
								.append(" ");
						sb.append(profileData).append(" ");
					}
				}
			}

			// write the lift output
			if (!SKIP_LIFT) {
				Text k = new Text();
				k.set(classLabel + "\t" + "lp");
				sb.append("}");
				v.set(sb.toString());
				m.write(k, v, "lift" + "/" + trainingdate + "/");
			}
		}
		// else if (prob.equals(Util.PROBABILITY_OF_CLASS_PATH))
		// {
		// String date = str[1];
		// String classLabel = str[2];
		// DoubleWritable v = new DoubleWritable();
		//
		// // for Yang one Day is one Session
		// double instancesPerClass = 1;
		//
		// Text k = new Text();
		//
		// k.set(classLabel);
		// v.set(instancesPerClass);
		// m.write("test", k, v, Util.INSTANCES_PER_CLASS_PATH + "/" + date +
		// "/");
		// }
		else if (prob.equals(Util.PROBABILITY_OF_CLASS_PATH)) {
			String date = str[1];
			String classLabel = str[2];
			DoubleWritable v = new DoubleWritable();
			int numInstancesAtDate = numInstances.get(date);

			int numClasses = context.getConfiguration().getInt(
					Util.CONF_NUM_CLASSES, 0);

			// loop through all values
			double instancesPerClass = 0;

			for (DoubleArrayWritable rr : values) {
				Writable[] w = rr.get();

				instancesPerClass += ((DoubleWritable) w[0]).get();

				Util.ping(context, TrainingReducer.class);
			}

			Text k = new Text();
			// k.set(classLabel);
			// v.set(Util.bayesPrH(instancesPerClass, numInstancesAtDate,
			// numClasses));
			// m.write("probOfClass",k, v, Util.PROBABILITY_OF_CLASS_PATH + "/"
			// + date + "/");

			k.set(classLabel);
			v.set(instancesPerClass);
			m.write("probOfClass", k, v, Util.INSTANCES_PER_CLASS_PATH + "/"
					+ date + "/");
		}
	}

	public void readDailyDocumentFrequencies(Context context,
			String trainingDate) throws IOException {
		// avoid reading ddfs if we already have read them in a previous run
		if (dateOfDdfs != null && dateOfDdfs.equals(trainingDate))
			return;
		dateOfDdfs = trainingDate;

		String line = null;
		String rr[] = null;
		Path file = null;
		// readDailyCounter++;
		Configuration conf = context.getConfiguration();

		Path path;
		path = new Path(conf.get(Util.CONF_HEADER_PATH) + "/"
				+ DataSetHeader.DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY);

		FileSystem fs = FileSystem.get(conf);
		_dailyDocFreqMap = new TIntFloatHashMap();

		for (FileStatus fileStatus : fs.listStatus(path)) {
			if (fileStatus.isDir()
					|| !(fileStatus.getPath().getName()
							.startsWith(trainingDate)))
				continue;

			file = fileStatus.getPath();

			LOG.info("reading ddfs for " + trainingDate);
			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			int i = 0;

			while (true) {
				line = reader.readLine();

				if (i++ % 10000 == 0) {
					Util.ping(context, YangTestMapper.class);
					i = 0;
				}
				if (line == null) {
					break;
				}
				// line = 5585 3
				rr = line.split("\t");

				// rr[0] host
				// rr[1] dailyDocfreq

				String host = rr[0];
				int docfreq = Integer.parseInt(rr[1]);
				float tmp = _dailyDocFreqMap.get(Integer.parseInt(host));
				_dailyDocFreqMap.put(Integer.parseInt(host), docfreq + tmp);
			}
		}

		line = null;
		rr = null;
	}

	@Override
	public void setup(Context context) {
		try {
			Configuration conf = context.getConfiguration();
			if (conf.get(Util.CONF_CLASSIFIER).equals("support")) {
				SKIP_SUPPORT = false;
				SKIP_LIFT = true;
			} else if (conf.get(Util.CONF_CLASSIFIER).equals("lift")) {
				SKIP_SUPPORT = true;
				SKIP_LIFT = false;
			}

			// initialize the multiple outputs handler
			m = new MultipleOutputs(context);
			_candidateMap = Util.readCandidatePattern(context);
			// read number of instances from dataset header
			numInstances = DataSetHeader.getNumInstances(context
					.getConfiguration());
			// readOverallDocumentFrequencies(context);

			// neu: alle auf einmal laden!
			// readDailyDocumentFrequencies(context);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
