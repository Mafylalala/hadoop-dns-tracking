package de.sec.dns.dataset;

import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TIntShortHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.ardverk.collection.PatriciaTrie;
import org.ardverk.collection.StringKeyAnalyzer;
import org.ardverk.collection.Trie;

import de.sec.dns.util.CompactCharSequence;
import de.sec.dns.util.HostIndex;
import de.sec.dns.util.TextPair;
import de.sec.dns.util.Util;

/**
 * The reducer for {@link GenerateDataSetTool}. It fetches entries from the
 * mapper or combiner.
 * 
 * Input Key: TextPair(date, user)
 * 
 * @author Christian Banse
 */
public class GenerateDataSetReducer extends Reducer<TextPair, Text, Text, Text> {
	class MinMaxPair {
		public int maxValue;
		public int minValue;

		public MinMaxPair(int min, int max) {
			minValue = min;
			maxValue = max;
		}
	}

	/**
	 * Save RAM by lazily reading in the idf entries from the filesystem and
	 * purging them if they are not needed any more. This is only implemented
	 * for daily document frequencies, but not for overall document frequencies
	 * (because they should fit into RAM).
	 * 
	 * Note: Setting this to true will cause some performance degradation, but
	 * it may be necessary to process the full dataset, i.e. because we cannot
	 * store idfs for all 150 days in RAM at once.
	 * 
	 * This should be set to true from now.
	 */
	private static final boolean LAZY_READING_OF_IDF = true;

	/**
	 * Output RAW files? Otherwise only tfn files are written
	 */
	private static final boolean OUTPUT_RAW_FILES = true;

	/**
	 * Counts how many entries have already been reduced.
	 */
	private static int counter = 0;

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory
			.getLog(InstanceRecordReader.class);

	/**
	 * Set to true to throw an exception whenever a host is not found in the
	 * host cache.
	 */
	private static final boolean THROW_FATAL_ERROR_IF_HOST_NOT_FOUND_IN_INDEX = false;

	/**
	 * Add second level domains automatically?
	 */
	private boolean ADD_SECOND_LEVEL_DOMAINS = false;

	/**
	 * Hashmap containing hostnames and their daily docfreq of the current day
	 */
	HashMap<String, TIntShortHashMap> dailyDocFreqMap = new HashMap<String, TIntShortHashMap>();

	/**
	 * Map containing host indices and their idf for the whole dataset. The
	 * object is initialized in setup() once the number of hosts is known
	 */
	private TIntFloatHashMap overallInverseDocFreqMap;

	/**
	 * A java crypto digest used to create md5 hashes.
	 */
	private MessageDigest digest;

	/**
	 * The host index.
	 */
	private HostIndex hostIndex = HostIndex.getInstance();

	/**
	 * If set to true, all actual frequencies will be neglected and substituted
	 * with freq = 1
	 */
	private boolean IGNORE_FREQUENCIES = false;

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text k = new Text();

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<Text, Text> m;

	/**
	 * Do not add user profiles which contain more than the given threshold. Set
	 * to -1 to ignore this parameter
	 */
	// int MAXIMUM_REQUESTS_PER_PROFILE = 6330;
	private int MAXIMUM_REQUESTS_PER_PROFILE = -1;

	/**
	 * Do not add hosts to the histogram of a user profile which occur less than
	 * the given threshold.
	 */
	private int MINIMUM_HOST_OCCURRENCE_FREQUENCY = 0;

	/**
	 * Do not add user profiles which contain less than the given threshold. Set
	 * to 0 to ignore this parameter
	 */
	// bottom 5% am 20.04.
	// private final int MINIMUM_REQUESTS_PER_PROFILE = 156;
	// test mit 1000, um vergleichbare Mindestnutzung bei unterschiedl. cutoff
	// zu haben
	private int MINIMUM_REQUESTS_PER_PROFILE = 0;

	/**
	 * Stores min and max requests per session (=string)
	 */
	private HashMap<String, MinMaxPair> minMaxRequestsMap = new HashMap<String, MinMaxPair>();

	/**
	 * Stores number of Instances per session
	 */
	private HashMap<String, Integer> numInstances;

	/**
	 * Stores overall number of instances in dataset (needed for calculation of
	 * overall idf)
	 */
	private int totalNumInstances;

	private boolean USE_DAILY_DOCUMENT_FREQUENCIES;

	private boolean USE_OVERALL_DOCUMENT_FREQUENCIES;

	private boolean USE_DYNAMIC_REQUEST_RANGE_INDEX;

	private boolean DYNAMIC_REQUEST_RANGE_SKIP_ONLY_TOP_USERS;

	private boolean DYNAMIC_REQUEST_RANGE_SKIP_ONLY_BOTTOM_USERS;

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

	public void readDailyDocumentFrequencies(Context context)
			throws IOException {
		String line = null;
		String rr[] = null;
		// TIntByteHashMap map = null;
		Path file = null;

		Configuration conf = context.getConfiguration();

		Path path = new Path(conf.get(Util.CONF_HEADER_PATH) + "/"
				+ DataSetHeader.DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY);

		FileSystem fs = FileSystem.get(conf);

		LOG.info("starting to populate idf map...");
		Util.getMemoryInfo(HostIndex.class);

		dailyDocFreqMap = new HashMap<String, TIntShortHashMap>();

		for (FileStatus fileStatus : fs.listStatus(path)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();
			if (file.getName().length() < 16) {
				continue;
			}
			String date = file.getName().substring(0, 16);

			// LOG.info("reading from " + file + "...");

			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			int i = 0;
			while (true) {
				line = reader.readLine();

				if (i++ % 10000 == 0) {
					Util.ping(context, GenerateDataSetReducer.class);
					i = 0;
				}

				if (line == null) {
					break;
				}

				rr = line.split("\t");

				// rr[0] host
				// rr[1] dailydocfreq

				String host = rr[0];
				short docfreq = Short.parseShort(rr[1]);

				TIntShortHashMap dfmap = dailyDocFreqMap.get(date);
				if (dfmap == null) {
					dfmap = new TIntShortHashMap();
					dailyDocFreqMap.put(date, dfmap);
				}
				dfmap.put(Integer.parseInt(host), docfreq);

			}
		}

		line = null;
		rr = null;
		LOG.info("idf map populated done");
		Util.getMemoryInfo(HostIndex.class);

	}

	public TIntShortHashMap readDailyDocumentFrequenciesForOneDay(
			Context context, String requestedDate) throws IOException {
		String line = null;
		String rr[] = null;
		// TIntByteHashMap map = null;
		Path file = null;

		if (dailyDocFreqMap == null)
			dailyDocFreqMap = new HashMap<String, TIntShortHashMap>();

		TIntShortHashMap cachedCopy = dailyDocFreqMap.get(requestedDate);
		if (cachedCopy != null)
			return cachedCopy;

		// clean up the map if it has grown too large
		// Runtime rt = Runtime.getRuntime();
		// if(dailyDocFreqMap.size() >= MAX_DAILY_DOC_FREQ_MAP_SIZE) {
		/*
		 * if(((float)rt.freeMemory() / (float)rt.maxMemory()) < 0.1) { // if
		 * the amount of free memory drops too much, we start dropping idf
		 * entries if(dailyDocFreqMap.size()>0) { String [] keys = (String[])
		 * dailyDocFreqMap.keySet().toArray(new String[dailyDocFreqMap.size()]);
		 * LOG.info("removing one entry in idf map: " + keys[0]);
		 * dailyDocFreqMap.remove(keys[0]); } }
		 */
		dailyDocFreqMap.clear();

		Configuration conf = context.getConfiguration();

		Path path = new Path(conf.get(Util.CONF_HEADER_PATH) + "/"
				+ DataSetHeader.DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY);

		FileSystem fs = FileSystem.get(conf);

		LOG.info("starting to populate idf map for date " + requestedDate
				+ "...");

		for (FileStatus fileStatus : fs.listStatus(path)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();
			if (file.getName().length() < 16) {
				continue;
			}

			String date = file.getName().substring(0, 16);

			if (!date.equals(requestedDate))
				continue;

			// LOG.info("reading from " + file + "...");

			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			int i = 0;
			while (true) {
				line = reader.readLine();

				if (i++ % 10000 == 0) {
					Util.ping(context, GenerateDataSetReducer.class);
					i = 0;
				}

				if (line == null) {
					break;
				}

				rr = line.split("\t");

				// rr[0] host
				// rr[1] dailydocfreq

				String host = rr[0];
				short docfreq = Short.parseShort(rr[1]);

				TIntShortHashMap dfmap = dailyDocFreqMap.get(date);
				if (dfmap == null) {
					dfmap = new TIntShortHashMap();
					dailyDocFreqMap.put(date, dfmap);
				}
				dfmap.put(Integer.parseInt(host), docfreq);

			}
		}

		line = null;
		rr = null;
		LOG.info("idf map populated");

		return dailyDocFreqMap.get(requestedDate);
	}

	public void readOverallDocumentFrequencies(Context context)
			throws IOException {
		String line = null;
		String rr[] = null;
		Path file = null;

		Configuration conf = context.getConfiguration();

		Path path = new Path(conf.get(Util.CONF_HEADER_PATH) + "/"
				+ DataSetHeader.OVERALL_DOCUMENT_FREQUENCY_INDEX_DIRECTORY);

		FileSystem fs = FileSystem.get(conf);
		overallInverseDocFreqMap = new TIntFloatHashMap();

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

				if (i++ % 10000 == 0) {
					Util.ping(context, GenerateDataSetReducer.class);
					i = 0;
				}

				if (line == null) {
					break;
				}

				rr = line.split("\t");

				// rr[0] host
				// rr[1] overallDocfreq

				String host = rr[0];
				int docfreq = Integer.parseInt(rr[1]);

				float idf = (float) Math.log10((float) totalNumInstances
						/ (float) docfreq);
				overallInverseDocFreqMap.put(Integer.parseInt(host), idf);
			}
		}

		line = null;
		rr = null;
	}

	@Override
	protected void reduce(TextPair dateAndUser, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		CompactCharSequence md5Host = null;
		String host = null;
		int value = 0;
		int pingcounter = 0;

		counter++;
		if ((counter % 100) == 0) {
			LOG.info("reduced " + counter + " records; current: "
					+ dateAndUser.getFirst() + "@" + dateAndUser.getSecond());
		}

		// 1.
		//
		// Populate a Trie (prefix tree) with all host names from a given
		// profile
		// We insert the hostnames into the trie BEFORE we hash them.
		// This costs more money, but saves repeatedly calculating hashes for
		// the same domain
		// (which should save considerable time)
		// TODO:E anschauen
		Trie<String, Integer> map = new PatriciaTrie<String, Integer>(
				StringKeyAnalyzer.INSTANCE);

		// iterate over the values of the current user and put the occurences
		// into the Trie
		int valuecount = 0;

		for (Text val : values) {
			try {
				if ((counter++ % 1000) == 0) {
					Util.ping(context, GenerateDataSetReducer.class);
				}

				String[] rr = val.toString().split(" "); // needed to handle
				// reduce and
				// combine with the
				// same code
				host = rr[0];

				if (host == null) {
					LOG.error("!! Host is null for user "
							+ dateAndUser.getSecond());
					throw new IOException("!! Host is null for user "
							+ dateAndUser.getSecond());
				}

				// entry comes from the mapper, set the occurence to 1
				if (rr.length == 1) {
					value = 1;
				} else {
					// entry comes from the combiner and is already summed up?
					// -> use the value form the combiner
					value = Integer.parseInt(rr[1]);

					if (IGNORE_FREQUENCIES) {
						value = 1;
					}
				}

				// put it in the hashmap
				Integer count = map.get(host);
				if (count == null) {
					map.put(host, value);
				} else {
					if (!IGNORE_FREQUENCIES) {
						map.put(host, count + value);
					}
				}

				if (ADD_SECOND_LEVEL_DOMAINS) {
					String secondLevelDomain = Util.getSecondLevelDomain(host);
					if (secondLevelDomain != null) {
						count = map.get(secondLevelDomain);
						if (count == null) {
							map.put(secondLevelDomain, value);
						} else {
							if (!IGNORE_FREQUENCIES) {
								map.put(secondLevelDomain, count + value);
							}
						}
					}
				}

				// keep track of size of user profile
				valuecount += value;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// now the trie contains all hostnames the user visited + their
		// occurence frequency

		// reject profiles that do not meet the requested number of requests
		if ((valuecount < MINIMUM_REQUESTS_PER_PROFILE)
				|| ((MAXIMUM_REQUESTS_PER_PROFILE >= 0) && (valuecount > MAXIMUM_REQUESTS_PER_PROFILE))) {
			return;
		}

		// 2.
		// Output the resulting profiles, first raw, then tfn transformed

		// TextPair(date, user)

		String user = dateAndUser.getSecond().toString();
		String date = dateAndUser.getFirst().toString();

		LOG.info("Processing " + user + " @ " + date);

		if (USE_DYNAMIC_REQUEST_RANGE_INDEX) {
			MinMaxPair p = minMaxRequestsMap.get(date);
			if (DYNAMIC_REQUEST_RANGE_SKIP_ONLY_TOP_USERS) {
				if (valuecount > p.maxValue) {
					k.set(user); // will not be written out!
					v.set(user+"\t"+date);
					m.write(k, v, "droppedUsers/users");
					return;
				}
			} else if (DYNAMIC_REQUEST_RANGE_SKIP_ONLY_BOTTOM_USERS) {
				if (valuecount < p.minValue) {
					k.set(user);
					v.set(user+"\t"+date);
					m.write(k, v, "droppedUsers/users");
					return;
				}
			} else {
				if ((valuecount < p.minValue) || (valuecount > p.maxValue)) {
					k.set(user);
					v.set(user+"\t"+date);
					m.write(k, v, "droppedUsers/users");
					return;
				}
			}
		}

		StringBuilder builder = new StringBuilder();

		// vector length variable for cosine normalization
		double vlength = 0;

		int numOfHostsInProfile = 0;

		//
		// Iterate over the hashmap and build the RAW string
		// Furthermore, calculate vector length used for
		// normalization in the next step

		// precompute the idf of a host occurring only once
		float idfOfHostOccuringOneTime = (float) Math
				.log10((float) totalNumInstances / 1);

		pingcounter = 0;

		if (LAZY_READING_OF_IDF && USE_DAILY_DOCUMENT_FREQUENCIES) {
			readDailyDocumentFrequenciesForOneDay(context, date);
		}

		TIntShortHashMap dfmap = dailyDocFreqMap.get(date);

		for (String key : map.keySet()) {
			if ((pingcounter++ % 1000) == 0) {
				Util.ping(context, GenerateDataSetReducer.class);
			}

			// get hash of the host for lookup in index
			md5Host = Util.getHash(digest, key);

			// retrieve the index of the host from the host index
			int i = hostIndex.getIndexPosition(md5Host);

			double freq = map.get(key);

			double df = 1;
			if (USE_DAILY_DOCUMENT_FREQUENCIES) {
				// get dailydocfreq for host "key"
				int numInstancesAtDate = numInstances.get(date);
				if (dfmap.contains(i)) {
					int numDocsWithHost = dfmap.get(i);
					df = Math.log10((float) numInstancesAtDate
							/ (float) numDocsWithHost);
				} else {
					LOG.info("Skipping host " + key + " because its index ("
							+ i + ") not found in dfmap for date " + date);
					/*
					 * FIXME probably it is not a good idea to skip hosts that
					 * haven't appeared on the previous day altogether because
					 * this may change their influence in comparison to the
					 * remaining hosts inadvertantly. Maybe we should pretend
					 * that such hosts occured once on the previous day? Or we
					 * could try some sort of laplace (add 1 to all frequencies)
					 * smoothing at this point.
					 */
				}
			}

			float idf = 1;
			if (USE_OVERALL_DOCUMENT_FREQUENCIES) {
				// get idf for host with index i
				if (overallInverseDocFreqMap.contains(i)) {
					idf = overallInverseDocFreqMap.get(i);
				} else {
					// the overallInverseDocumentFrequency files do not store
					// entries
					// for hosts which occurred only once; thus we have to
					// calculate the
					// idf on the fly here
					idf = idfOfHostOccuringOneTime;
				}
			}

			if (freq >= MINIMUM_HOST_OCCURRENCE_FREQUENCY) {
				if (i > -1) {
					if (OUTPUT_RAW_FILES) {
						builder.append(i).append(" ");
						builder.append(freq).append(" ");
					}
					numOfHostsInProfile++;
					// calculate the euclidian length of the vector
					vlength += Math.log10(1 + freq) * Math.log10(1 + freq) * df
							* df * idf * idf;
				} else {
					String errorMsg = "Fatal error: Could not find host " + key
							+ " / " + Util.getHexString(md5Host.getBytes())
							+ " in index!";
					if (THROW_FATAL_ERROR_IF_HOST_NOT_FOUND_IN_INDEX) {
						throw new IOException(errorMsg);
					} else {
						System.err.println(errorMsg);
					}
				}
			}
		}

		if (OUTPUT_RAW_FILES) {
			builder.insert(0, numOfHostsInProfile + " { ");
		}

		// calculate the euclidian length of the vector
		vlength = Math.sqrt(vlength);

		if (OUTPUT_RAW_FILES) {
			builder.append("} " + user.toString() + ",1"); // FIXME: use
			// ID_SEPARATOR
			// instead of ,
		}

		// DO NOT WRITE THE RAW DATA ANYMORE!!!
		// write the raw data

		if (OUTPUT_RAW_FILES) {
			k.set(user);
			v.set(builder.toString());
			m.write(k, v, "raw/" + date + "/data");
		}
		builder = null;

		// iterate over the values again and build the tfn string
		builder = new StringBuilder();
		builder.append(numOfHostsInProfile + " { ");

		pingcounter = 0;
		for (String key : map.keySet()) {
			if ((pingcounter++ % 1000) == 0) {
				Util.ping(context, GenerateDataSetReducer.class);
			}

			md5Host = Util.getHash(digest, key);

			int i = hostIndex.getIndexPosition(md5Host);

			// calculate the TFN value
			// Math.log10(1 + value) / (euclidian_length)
			double freq = map.get(key);

			double df = 1;
			if (USE_DAILY_DOCUMENT_FREQUENCIES) {
				// get dailydocfreq for host "key"
				int numInstancesAtDate = numInstances.get(date);
				if (dfmap.contains(i)) {
					int numDocsWithHost = dfmap.get(i);
					df = Math.log10((float) numInstancesAtDate
							/ (float) numDocsWithHost);
				} else {
					LOG.info("Skipping host " + key + " because its index ("
							+ i + ") not found in dfmap for date " + date);
				}
			}

			float idf = 1;
			if (USE_OVERALL_DOCUMENT_FREQUENCIES) {
				// get idf for host with index i
				if (overallInverseDocFreqMap.contains(i)) {
					idf = overallInverseDocFreqMap.get(i);
				} else {
					// the overallInverseDocumentFrequency files do not store
					// entries
					// for hosts which occurred only once; thus we have to
					// calculate the
					// idf on the fly here
					idf = idfOfHostOccuringOneTime;
				}
			}

			if (freq >= MINIMUM_HOST_OCCURRENCE_FREQUENCY) {
				// Note that vlength has been calculated above using the already
				// tf-idf-transformed frequencies. Thus, we can use it here
				// to normalize all values.
				// FIXME: Ideally, we would normalize to the average length
				// instead
				// of normalizing to unit (1) length to counter floating point
				// underflows.
				freq = (Math.log10(1 + freq) * df * idf) / vlength;
				if (i > -1) {
					builder.append(i).append(" ");
					builder.append(
							(double) ((int) (freq * 1000000000)) / 1000000000) // added
							// two
							// digits
							// for
							// more
							// precision
							.append(" ");
				} else {
					if (THROW_FATAL_ERROR_IF_HOST_NOT_FOUND_IN_INDEX) {
						throw new IOException(
								"Fatal error: Could not find host " + key
										+ " / "
										+ Util.getHexString(md5Host.getBytes())
										+ " in index!");
					}
				}
			}
		}

		builder.append("} " + user.toString() + Instance.ID_SEPARATOR + "1");

		k.set(user);
		v.set(builder.toString());

		// write the tfn data
		m.write(k, v, "tfn/" + date + "/data");
		builder = null;

		map = null;

		// print some memory info every once in a while
		if ((counter % 1000) == 0) {
			Util.getMemoryInfo(GenerateDataSetReducer.class);
		}

	}

	@Override
	public void setup(Context context) {
		try {
			Configuration conf = context.getConfiguration();
			MINIMUM_HOST_OCCURRENCE_FREQUENCY = conf.getInt(
					Util.CONF_MIN_HOST_OCCURRENCE_FREQUENCY, 0);
			MINIMUM_REQUESTS_PER_PROFILE = conf.getInt(
					Util.CONF_MIN_REQUESTS_PER_PROFILE, 0);
			MAXIMUM_REQUESTS_PER_PROFILE = conf.getInt(
					Util.CONF_MAX_REQUESTS_PER_PROFILE, -1);
			ADD_SECOND_LEVEL_DOMAINS = conf.getBoolean(
					Util.CONF_ADD_SECOND_LEVEL_DOMAINS, false);
			USE_DYNAMIC_REQUEST_RANGE_INDEX = conf.getBoolean(
					Util.CONF_USE_DYNAMIC_REQUEST_RANGE_INDEX,
					Util.DEFAULT_USE_DYNAMIC_REQUEST_RANGE_INDEX);
			DYNAMIC_REQUEST_RANGE_SKIP_ONLY_TOP_USERS = conf.getBoolean(
					Util.CONF_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_TOP_USERS,
					Util.DEFAULT_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_TOP_USERS);
			DYNAMIC_REQUEST_RANGE_SKIP_ONLY_BOTTOM_USERS = conf.getBoolean(
					Util.CONF_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_BOTTOM_USERS,
					Util.DEFAULT_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_BOTTOM_USERS);

			IGNORE_FREQUENCIES = conf.getBoolean(Util.CONF_IGNORE_FREQUENCIES,
					Util.DEFAULT_IGNORE_FREQUENCIES);
			USE_DAILY_DOCUMENT_FREQUENCIES = conf.getBoolean(
					Util.CONF_USE_DAILY_DOCUMENT_FREQUENCIES,
					Util.DEFAULT_USE_DAILY_DOCUMENT_FREQUENCIES);
			USE_OVERALL_DOCUMENT_FREQUENCIES = conf.getBoolean(
					Util.CONF_USE_OVERALL_DOCUMENT_FREQUENCIES,
					Util.DEFAULT_USE_OVERALL_DOCUMENT_FREQUENCIES);

			// read number of instances from dataset header for doc-freq
			numInstances = DataSetHeader.getNumInstances(context
					.getConfiguration());

			// initialize the digest
			this.digest = MessageDigest.getInstance("MD5");

			// populate the host index
			if (!hostIndex.isPopulated()) {
				hostIndex.init(context);
				hostIndex.populateIndex();
			}

			// if lazy reading is turned on, we read the IDF within the
			// reduce method
			if (!LAZY_READING_OF_IDF) {
				if (USE_DAILY_DOCUMENT_FREQUENCIES) {
					readDailyDocumentFrequencies(context);
				}
			}

			// determine total number of instances in dataset, which is
			// needed to calculate the idf values
			if (USE_OVERALL_DOCUMENT_FREQUENCIES) {
				for (int numInstancesPerDay : numInstances.values()) {
					totalNumInstances += numInstancesPerDay;
				}
				readOverallDocumentFrequencies(context);
			}

			// initialize the multiple outputs handler
			m = new MultipleOutputs<Text, Text>(context);

			// read rangeIndex
			Path path = new Path(context.getConfiguration().get(
					Util.CONF_HEADER_PATH));
			FileSystem fs = FileSystem.get(context.getConfiguration());
			for (FileStatus s : fs.listStatus(path.suffix("/"
					+ DataSetHeader.REQUEST_RANGE_INDEX_DIRECTORY))) {
				if (!s.getPath().getName().startsWith("header")) {
					continue;
				}

				FSDataInputStream stream = fs.open(s.getPath());
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(stream));

				String line;
				while ((line = reader.readLine()) != null) {
					String[] rr = line.split("\t");
					String date = rr[0];
					String[] minMax = rr[1].split(" ");
					minMaxRequestsMap.put(
							date,
							new MinMaxPair(Integer.parseInt(minMax[0]), Integer
									.parseInt(minMax[1])));
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
