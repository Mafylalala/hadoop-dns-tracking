package de.sec.dns.training;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.test.YangTestMapper;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * The reducer class for {@link CandidatePatternTool}. It sums the occurende of
 * a host for each User and then selects the Top Hosts.
 * 
 * @author Elmo Randschau
 */
public class CandidatePatternReducer extends
		Reducer<Text, DoubleArrayWritable, Text, IntWritable> {
	private static int counter = 0;
	Configuration conf;
	private static int NUMBER_OF_TOP_PATTERN;
	private static String DATASET_FILTER_METHOD;
	private String dateOfDdfs;
	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory
			.getLog(CandidatePatternReducer.class);
	private static final Object METHOD_SUPPORT = "top-patterns-support";
	private static final Object METHOD_LIFT = "top-patterns-lift";

	/**
	 * Map containing the number of instances (users) for a Date
	 */
	private HashMap<String, Integer> numInstances = new HashMap<String, Integer>();
	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<Text, IntWritable> m;

	// private HashMap<String, Integer> numInstances = new HashMap<String,
	// Integer>();
	/**
	 * Map containing host indices and their idf for the whole dataset. The
	 * object is initialized in setup() once the number of hosts is known hostID
	 * <-> occurence
	 */
	private TIntFloatHashMap _dailyDocFreqMap;
	/**
	 * A temporary variable for the mapreduce value.
	 */
	private IntWritable v = new IntWritable();
	private Text k = new Text();

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
	protected void reduce(Text dateAndOrClassLabel,
			Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		int numSession = 0;
		// retrieve the Number of CandidatePatterns
		NUMBER_OF_TOP_PATTERN = conf.getInt(Util.CONF_NUM_TOP_PATTERNS, 10);

		// TODO: implement different methods of selection the TopPatterns
		DATASET_FILTER_METHOD = conf.get(Util.CONF_DATASET_FILTER_METHOD,
				"top-patterns-support");

		/**
		 * a map containing attributeIndex and it's occurrence from the Class
		 * and all it's instances on the given date
		 */
		TIntDoubleHashMap attributeOccurenceMap = new TIntDoubleHashMap();
		HashMap<Double, HashSet<Integer>> metricAttributeMAP = null;

		/**
		 * dateAndOrClassLabel will contain date and ClassLabel in normal cases,
		 * and only the ClassLaben if we use a global view on the TopPatterns
		 */
		String[] str = dateAndOrClassLabel.toString().split(" ");

		String date = null;
		String classLabel = null;
		if (str.length == 1) {
			classLabel = str[0];
		} else {
			date = str[0];
			classLabel = str[1];
		}
		// set the ClassLabel as the output Key
		k.set(classLabel);
		if ((counter++ % 10) == 0) {
			LOG.info("reducing instance " + classLabel);
		}

		// loop through all values, put them into a hashmap
		int valueCounter = 0;
		for (DoubleArrayWritable rr : values) {
			Writable[] w = rr.get();
			// w[0] = attributeIndex
			// w[1] = numOccurences

			double occ = ((DoubleWritable) w[1]).get();
			int attributeIndex = (int) ((DoubleWritable) w[0]).get();
			if (attributeIndex == Util.SESSION_COUNT) {
				numSession++;
				continue;
			}

			double value = attributeOccurenceMap.get(attributeIndex);
			attributeOccurenceMap.put(attributeIndex, occ + value);

			Util.ping(context, CandidatePatternReducer.class);

			valueCounter++;
			if (valueCounter % 10000 == 0)
				Util.getMemoryInfo(CandidatePatternReducer.class);
		}
		// the attributeOccurenceMap now contains all visited hosts from one
		// user

		if ((counter % 10) == 0) {
			LOG.info("Num Sessions : " + numSession + " for Class: "
					+ classLabel);
		}
		Double[] rankedCandidatePatternArray;

		if (DATASET_FILTER_METHOD.equals(CandidatePatternReducer.METHOD_LIFT)) {
			double D = (double) numInstances.get(date);
			readDailyDocumentFrequencies(context, date);
			HashMap<Double, HashSet<Integer>> liftAttributeMAP = new HashMap<Double, HashSet<Integer>>();

			for (TIntDoubleIterator it = attributeOccurenceMap.iterator(); it
					.hasNext();) {
				it.advance();
				int tmp = it.key();
				double Dpj = (double) _dailyDocFreqMap.get(tmp);
				double d1 = it.value();
				double d2 = Dpj / D;
				double lp = d1 / d2;
				lp = (double) ((int) (lp * 1000000)) / 1000000;
				HashSet<Integer> s = liftAttributeMAP.get(lp);

				// if we don't want to add same important TopPatterns, or there
				// is no TopPattern with
				// this occurrence, create a new Set<hostID> and add it to the
				// occurenceAttributeMap
				if (conf.getBoolean(Util.CONF_NO_SAME_TOPPATTERNS, false)
						|| (s == null)) {
					s = new HashSet<Integer>();
					liftAttributeMAP.put(lp, s);
				}
				s.add(it.key());
			}
			// set the metricAttributeMAP, this Map will then be sorted and the
			// X-TopPatterns are
			// selected
			metricAttributeMAP = liftAttributeMAP;
		}

		if (DATASET_FILTER_METHOD
				.equals(CandidatePatternReducer.METHOD_SUPPORT)) {
			HashMap<Double, HashSet<Integer>> occurenceAttributeMAP = new HashMap<Double, HashSet<Integer>>();

			for (TIntDoubleIterator it = attributeOccurenceMap.iterator(); it
					.hasNext();) {
				it.advance();
				HashSet<Integer> s = occurenceAttributeMAP.get(it.value());

				// if we don't want to add same important TopPatterns, or there
				// is no TopPattern with
				// this occurrence, create a new Set<hostID> and add it to the
				// occurenceAttributeMap
				if (conf.getBoolean(Util.CONF_NO_SAME_TOPPATTERNS, false)
						|| (s == null)) {
					s = new HashSet<Integer>();
					occurenceAttributeMAP.put(it.value(), s);
				}
				s.add(it.key());
			}

			metricAttributeMAP = occurenceAttributeMAP;
		}

		rankedCandidatePatternArray = metricAttributeMAP.keySet().toArray(
				new Double[0]);
		java.util.Arrays.sort(rankedCandidatePatternArray);

		int i = rankedCandidatePatternArray.length - 1;
		int count = 0;
		do {

			HashSet<Integer> s = metricAttributeMAP
					.get(rankedCandidatePatternArray[i]);
			for (Integer hostID : s) {
				if (!(count < NUMBER_OF_TOP_PATTERN))
					return;
				v.set(hostID);
				if (conf.getBoolean(Util.CONF_GLOBAL_TOP_PATTERNS, false)) {
					m.write(k, v, "global-candidate-patterns");
				} else {
					m.write(k, v, date);
				}
				count++;
				i--;
			}
		} while ((count < NUMBER_OF_TOP_PATTERN) && (i >= 0));
	}

	@Override
	public void setup(Context context) {
		try {
			conf = context.getConfiguration();
			// initialize the multiple outputs handler
			m = new MultipleOutputs<Text, IntWritable>(context);
			// read number of instances from dataset header
			numInstances = DataSetHeader.getNumInstances(context
					.getConfiguration());
		} catch (Exception e) {
			e.printStackTrace();
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

}
