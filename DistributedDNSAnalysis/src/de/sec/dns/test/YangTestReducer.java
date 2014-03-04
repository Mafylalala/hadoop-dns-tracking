package de.sec.dns.test;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TDoubleIntHashMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map.Entry;

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
import de.sec.dns.dataset.Instance;
import de.sec.dns.util.ClassIndex;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * The reducer class for {@link YangTestTool}. It generates the lift or support
 * based profiles and then compares the profile to the existing Profiles from
 * the Training days
 * 
 * @author Elmo Randschau
 */
public class YangTestReducer extends
		Reducer<Text, DoubleArrayWritable, Text, Text> {

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(YangTestReducer.class);

	private static boolean SKIP_SUPPORT = false;
	private static boolean SKIP_LIFT = true;

	/**
	 * The class index.
	 */
	private ClassIndex classIndex = ClassIndex.getInstance();

	private Text newClassInstanceID;
	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<Text, Text> m;

	/**
	 * Map containing host indices and their idf for the whole dataset. The
	 * object is initialized in setup() once the number of hosts is known hostID
	 * <-> occurence
	 */
	private TIntFloatHashMap _dailyDocFreqMap;
	private String dateOfDdfs;
	// remember the date of the ddfs so we can avoid re-reading them
	// unnecessarily

	private HashMap<Integer, TIntDoubleHashMap> _oldProfileMap = new HashMap<Integer, TIntDoubleHashMap>();
	private String dateOfOldProfiles;
	// we remember the date of the currently loaded oldProfile

	private DoubleWritable[] _newProfile;
	/**
	 * Map containing Distance - OldClassIndex(ClassLabel)
	 */
	private TDoubleIntHashMap _mapping;

	/**
	 * The current <i>Context</i>
	 */
	private Context context;

	private HashMap<String, Integer> numInstances = new HashMap<String, Integer>();

	private HashMap<String, TIntArrayList> _candidateMap;

	private DoubleWritable one = new DoubleWritable(1);
	private DoubleWritable zero = new DoubleWritable(0);

	@Override
	protected void reduce(Text newClassLabelDate,
			Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		// input key:classLabel + "\t" + date value: indexHost - occ
		/**
		 * map containing HostID - occurrence
		 */
		TIntDoubleHashMap map = new TIntDoubleHashMap();

		String[] str = newClassLabelDate.toString().split("\t");
		String date = str[1];
		String newClassLabel = str[0];
		newClassInstanceID = new Text(newClassLabel + ",1");
		String trainingDate = context.getConfiguration().get(
				Util.CONF_TRAINING_DATE);

		readDailyDocumentFrequencies(context, trainingDate);

		LOG.info("reducing instance " + newClassLabel);

		// loop through all values, put them into an hashmap
		// generates a map containing all Hosts for one User and one Session
		int valueCounter = 0;
		for (DoubleArrayWritable rr : values) {
			Writable[] w = rr.get();
			// w[0] = attributeIndex
			// w[1] = numOccurences in RAW format
			double occ = ((DoubleWritable) w[1]).get();

			int attributeIndex = (int) ((DoubleWritable) w[0]).get();

			double value = map.get(attributeIndex);
			map.put(attributeIndex, occ + value);

			Util.ping(context, YangTestReducer.class);

			valueCounter++;
			if (valueCounter % 10000 == 0)
				Util.getMemoryInfo(YangTestReducer.class);
		}
		map.put(Util.MISSING_ATTRIBUTE_INDEX, 0.0);

		// the withinUserStrength "Vector" hast the size of the candidatePattern
		DoubleWritable[] withinUserStrength = new DoubleWritable[_candidateMap
				.get(trainingDate).size()];

		for (int i = 0; i < _candidateMap.get(trainingDate).size(); i++) {
			// TODO: if we ever want to use more than one TestInstance we
			// need to do actual math here and not a binary decision ...
			withinUserStrength[i] = map.containsKey(_candidateMap.get(
					trainingDate).get(i)) ? one : zero;
			// Util.ping(context, TestYangReducer.class);
		}

		// generate support profile
		if (!SKIP_SUPPORT) {
			_newProfile = withinUserStrength;
			// now the profile is complete

			readProfiles(context, trainingDate, "support");

			_mapping = new TDoubleIntHashMap();
			int oldClassIndex = -1;
			double dist = -1;
			for (Entry<Integer, TIntDoubleHashMap> e : _oldProfileMap.entrySet()) {
				oldClassIndex = e.getKey().intValue();
				dist = compareProfiles(e.getValue(), _newProfile);
				//LOG.info("newClass: " + newClassInstanceID + " <-> oldClass: "
				//		+ classIndex.getClassname(oldClassIndex)
				//		+ " - distance: " + Util.format(dist));
				// put the distance and the oldClassLabel to the mapping
				// if two Classes have the same distance we simply choose one
				// because we can't
				// distinguish between them at all
				_mapping.put(dist, oldClassIndex);
			}

			int minIndex = -1;
			double minDist = Double.MAX_VALUE;
			int index;
			double distance;
			for (int i = 0; i < _mapping.keys().length; i++) {
				distance = _mapping.keys()[i];
				index = _mapping.get(distance);
				if (distance < minDist) {
					minDist = distance;
					minIndex = index;
				}
			}

			m.write(newClassInstanceID,
					new Text(classIndex.getClassname(minIndex) + "\t"
							+ (float) minDist), Util.PREDICTED_CLASSES_PATH
							+ "/" + date + "/");
		}

		// calculate / retrieve the overall pattern strength
		if (!SKIP_LIFT) {
			_newProfile = new DoubleWritable[_candidateMap.get(trainingDate)
					.size()];
			Double[] overallPatternStrength = new Double[_candidateMap.get(
					trainingDate).size()];
			double D = (double) numInstances.get(trainingDate);

			for (int i = 0; i < _candidateMap.get(trainingDate).size(); i++) {
				double Dpj = (double) _dailyDocFreqMap.get(_candidateMap.get(
						trainingDate).get(i));
				overallPatternStrength[i] = new Double(Dpj / D);

				double t1 = withinUserStrength[i].get();
				double t2 = overallPatternStrength[i];
				double tmp = t1 / t2;
				tmp = (tmp >= 0.0) ? tmp : 0.0;

				_newProfile[i] = new DoubleWritable(tmp);
			}
			// now the profile is complete

			readProfiles(context, trainingDate, "lift");

			_mapping = new TDoubleIntHashMap();
			int oldClassIndex = -1;
			double dist = -1;
			for (Entry<Integer, TIntDoubleHashMap> e : _oldProfileMap.entrySet()) {
				oldClassIndex = e.getKey().intValue();
				dist = compareProfiles(e.getValue(), _newProfile);
				//LOG.info(" - newClass: " + newClassInstanceID + " - oldClass: "
				//		+ classIndex.getClassname(oldClassIndex) + " - dist: "
				//		+ dist);

				// put the distance and the oldClassLabel to the mapping
				// if two Classes have the same distance we simply choose one
				// because we can't
				// distinguish between them at all
				_mapping.put(dist, oldClassIndex);
			}

			int minIndex = -1;
			double minDist = Double.MAX_VALUE;
			int index;
			double distance;
			for (int i = 0; i < _mapping.keys().length; i++) {
				distance = _mapping.keys()[i];
				index = _mapping.get(distance);
				if (distance < minDist) {
					minDist = distance;
					minIndex = index;
				}

			}

			// ----- used for checking if users with the same smallest distance
			// occur
			// can safely be enabled without messing with the results (but takes
			// extra time)

			// for (int i = 0; i < _mapping.keys().length; i++)
			// {
			// distance = _mapping.keys()[i];
			// index = _mapping.get(distance);
			// if ((distance == minDist) && (index !=minIndex) )
			// {
			// minDist = 0.0;
			// }
			// }

			// increase the counters
			String[] rr = newClassInstanceID.toString().split(
					Instance.ID_SEPARATOR);
			String clName = rr[0];
			if (clName.equals(classIndex.getClassname(minIndex))) {
				context.getCounter(
						YangTestTool.TestCounter.CORRECTLY_CLASSIFIED)
						.increment(1);
			} else {
				context.getCounter(
						YangTestTool.TestCounter.NOT_CORRECTLY_CLASSIFIED)
						.increment(1);
			}

			m.write(newClassInstanceID,
					new Text(classIndex.getClassname(minIndex) + "\t"
							+ (float) minDist), Util.PREDICTED_CLASSES_PATH
							+ "/" + date + "/");
		}
	}

	private void readProfiles(Context context, String trainingDate, String mode) {
		// avoid reading Profiles if we already have read them in a previous run
		if (dateOfOldProfiles != null && dateOfOldProfiles.equals(trainingDate))
			return;
		dateOfOldProfiles = trainingDate;
		// LOG.error("readProfile was called");
		String line = null;
		String rr[] = null;
		Path file = null;
		//double[] oldProfileArray = null;

		Configuration conf = context.getConfiguration();
		String option = conf.get(Util.CONF_OPTIONS);

		Path path = new Path(conf.get(Util.CONF_TRAINING_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/"
				+ mode + "/" + trainingDate);
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);

			for (FileStatus fileStatus : fs.listStatus(path)) {
				if (fileStatus.isDir()) {
					continue;
				}

				file = fileStatus.getPath();
				FSDataInputStream in = fs.open(file);
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(in));

				int i = 0;
				while (true) {
					line = reader.readLine();
					if (i++ % 10 == 0) {
						Util.ping(context, YangTestReducer.class);
						i = 0;
					}

					if (line == null) {
						break;
					}
					// ID \t MODE \t CANDIDATEPATTERNSIZE \t { attributeIndex
					// value ... }
					// 16d_1 lp 208 { 353 1.12903 ......

					rr = line.split("\t");
					// rr = Util.veryFastSplit(line, '\t', 4);
					// rr[0] oldClassLabel
					// rr[1] lift/support
					// rr[2] candidatePattern Size
					// rr[3] { 5486 2.0 ....}

					int oldClassIndex = classIndex.getIndexPosition(rr[0]);
					// String readMode = rr[1];
					int candidatePatternSize = Integer.parseInt(rr[2]);

					// retrieve the Profile Data. should be
					// 2xcandidatePatternSize +2(for "{" and "}")
					String[] profileData = rr[3].split(" ");

					TIntDoubleHashMap oldProfileSparseArray = new TIntDoubleHashMap(profileData.length/2);
					
					int currentProfileDataPosition = 1;

					// loop through all CandidatePattern
					for (int j = 0; j < candidatePatternSize; j++) {
						// if this CandidatePattern is present in Profile set
						// it's value

						if ((_candidateMap.get(trainingDate).get(j) == Integer
								.parseInt(profileData[currentProfileDataPosition]))) {
							// the Value for this attributeIndex is at
							// "currentProfileDataPosition+1"
							oldProfileSparseArray.put(j, Double
									.parseDouble(profileData[currentProfileDataPosition + 1]));

							// if we are in save distance to the array end ...
							// advance...
							if ((currentProfileDataPosition != profileData.length - 3)) {
								// advance 2 positions to the next
								// attributeIndex
								currentProfileDataPosition += 2;
							}
						} else {
							// if this CandidatePattern is not present in the
							// Profile set it to "0.0"
							//oldProfileArray[j] = 0.0;
						}
					}

					_oldProfileMap.put(oldClassIndex, oldProfileSparseArray);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		line = null;
		rr = null;

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
				_dailyDocFreqMap.put(Integer.parseInt(host), docfreq);
			}
		}

		line = null;
		rr = null;
	}

	private double compareProfiles(TIntDoubleHashMap oldProfileSparseArray,
			DoubleWritable[] newProfile) {
		// calculate the euclidean distance between the two profiles

		// das machte nur Sinn als oldProfileArray ein Array war, das nicht sparse war
		//if (oldProfileArray.length != newProfile.length)
		//	return -1.0;

		double sum = 0;
		for (int i = 0; i < newProfile.length; i++) {
			double d1 = oldProfileSparseArray.get(i);
			double d2 = newProfile[i].get();
			double difference = d1 - d2;
			sum += difference * difference;
		}
		double distance = Math.sqrt(sum);
		Util.ping(context, YangTestReducer.class);
		// INFINITY should never! occur ... but it somehow did while debugging
		return (distance == Double.POSITIVE_INFINITY) ? 32000.0 : distance;
	}

	// private boolean readCandidatePattern(Context context) throws IOException
	// {
	// _candidateMap = new HashMap<String, TIntArrayList>();
	// TIntHashSet candidatePatternSet = new TIntHashSet();
	// String line = null;
	// String rr[] = null;
	// Path file = null;
	//
	// Configuration conf = context.getConfiguration();
	// String option = conf.get(Util.CONF_OPTIONS);
	//
	// Path path = new Path(conf.get(Util.CONF_BASE_PATH) + "/candidatepattern"
	// + "/"
	// + conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");
	//
	// FileSystem fs = FileSystem.get(conf);
	// String date = "";
	// // for each candidatePattern file (there should only be one after all)
	// for (FileStatus fileStatus : fs.listStatus(path))
	// {
	// if (fileStatus.isDir())
	// {
	// continue;
	// }
	//
	// file = fileStatus.getPath();
	// String s = file.getName();
	// if (s.startsWith("part-r-"))
	// {
	// continue;
	// }
	// date = s.split("-r")[0];
	// LOG.info("reading candidatePattern for " + date);
	// FSDataInputStream in = fs.open(file);
	// BufferedReader reader = new BufferedReader(new InputStreamReader(in));
	//
	// /* candidate pattern format
	// * UserID HostID
	// * 1016_1 24768
	// * 1016_1 4172
	// * 1016_1 13487
	// * 101_1 5925
	// * 101_1 4139
	// */
	//
	// // neue Datei => PatternSet leeren
	// candidatePatternSet.clear();
	// int i = 0;
	// while (true)
	// {
	// line = reader.readLine();
	//
	// if (i++ % 10000 == 0)
	// {
	// Util.ping(context, YangTestReducer.class);
	// i = 0;
	// }
	// if (line == null)
	// {
	// break;
	// }
	// rr = line.split("\t");
	// // rr[0] user
	// // rr[1] hostID
	//
	// // we just need the hostID
	// int hostID = Integer.parseInt(rr[1]);
	//
	// candidatePatternSet.add(hostID);
	// }
	//
	// // Hier muss man aufpassen: es gibt pro Tag mehrere Dateien, aus denen
	// die
	// // Top-Patterns eingelesen werden müssen.
	//
	// TIntArrayList newList = new TIntArrayList(candidatePatternSet.toArray());
	// if (!_candidateMap.containsKey(date))
	// {
	// _candidateMap.put(date, new TIntArrayList());
	// }
	// TIntArrayList existingPatterns = _candidateMap.get(date);
	// existingPatterns.addAll(newList);
	//
	// // wild hack to make sure that hosts are not inserted twice into
	// existingPatterns
	// // (can happen due to multiple files)
	// TIntHashSet uniqueHostIds = new TIntHashSet(existingPatterns);
	// existingPatterns.clear();
	// existingPatterns.addAll(uniqueHostIds);
	// existingPatterns.sort();
	// }
	//
	// line = null;
	// rr = null;
	// return true;
	// }

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
	public void setup(Context context) {
		this.context = context;

		try {
			Configuration conf = context.getConfiguration();
			if (conf.get(Util.CONF_CLASSIFIER).equals("support")) {
				SKIP_SUPPORT = false;
				SKIP_LIFT = true;
			} else if (conf.get(Util.CONF_CLASSIFIER).equals("lift")) {
				SKIP_SUPPORT = true;
				SKIP_LIFT = false;
			} else if (conf.get(Util.CONF_CLASSIFIER).equals("yang")) {
				SKIP_SUPPORT = false;
				SKIP_LIFT = false;
			}

			// initialize the multiple outputs handler
			m = new MultipleOutputs<Text, Text>(context);

			_candidateMap = Util.readCandidatePattern(context);

			if (!classIndex.isPopulated()) {
				classIndex.init(context.getConfiguration());
				classIndex.populateIndex();
			}
			LOG.info("reading class probabilities...");

			numInstances = DataSetHeader.getNumInstances(context
					.getConfiguration());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
