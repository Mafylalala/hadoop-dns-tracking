package de.sec.dns.util;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.GenerateDataSetReducer;

/**
 * A utility class for various operations.
 * 
 * @author Christian Banse
 */
public class Util {
	public static final String CONF_ADD_SECOND_LEVEL_DOMAINS = "naivebayes.dataset.add_second_level_domains";
	public static final String CONF_INCLUDE_ALL_STATIC_USERS = "naivebayes.dataset.include_all_static_users";
	public static final String CONF_BASE_PATH = "naivebayes.base.path";
	public static final String CONF_ANALYSIS_PATH = "naivebayes.analysis.path";
	public static final String CONF_DATASET_PATH = "naivebayes.dataset.path";
	public static final String CONF_DATASET_FILTER_PATH = "filter_dataset.path";
	public static final String CONF_DATASET_USER_WHITELIST_PATH = "naivebayes.dataset.user_whitelist.path";
	public static final String CONF_DATASET_USER_WHITELIST_ENABLED = "naivebayes.dataset.user_whitelist.enabled";
	public static final String CONF_DATASET_HOST_WHITELIST_PATH = "naivebayes.dataset.host_whitelist.path";
	public static final String CONF_DATASET_HOST_WHITELIST_ENABLED = "naivebayes.dataset.host_whitelist.enabled";
	public static final String CONF_DATASET_USER_BLACKLIST_PATH = "naivebayes.dataset.user_blacklist.path";
	public static final String CONF_DATASET_USER_BLACKLIST_ENABLED = "naivebayes.dataset.user_blacklist.enabled";
	public static final String CONF_DATASET_HOST_BLACKLIST_PATH = "naivebayes.dataset.host_blacklist.path";
	public static final String CONF_DATASET_HOST_BLACKLIST_ENABLED = "naivebayes.dataset.host_blacklist.enabled";
	public static final String CONF_OPEN_WORLD_NUM_TRAINING_INSTANCES = "naivebayes.open_world.num_training_instances";
	public static final String CONF_OPEN_WORLD_NUM_TEST_INSTANCES = "naivebayes.open_world.num_test_instances";
	public static final String CONF_OPEN_WORLD_USERLIST_PATH = "naivebayes.open_world.userlist_path";
	public static final String CONF_CROSS_VALIDATION_PATH = "naivebayes.cv.path";
	public static final String CONF_CROSS_VALIDATION_NUM_FOLDS = "naivebayes.cv.num_folds";
	public static final String CONF_CROSS_VALIDATION_NUM_OF_CLASSES = "naivebayes.cv.num_classes";
	public static final String CONF_CROSS_VALIDATION_NUM_INSTANCES_PER_CLASS = "naivebayes.cv.num_instances_per_class";
	public static final String CONF_CROSS_VALIDATION_NUM_TRAINING_INSTANCES_PER_CLASS = "naivebayes.cv.num_training_instances_per_class";
	public static final String CONF_CROSS_VALIDATION_SEED = "naivebayes.cv.seed";
	public static final String CONF_DOCUMENT_FREQUENCY_PATH = "naivebayes.document_frequency.path";;
	public static final String CONF_DOWNCASE_HOSTNAMES = "naivebayes.dataset.downcase_hostnames";
	public static final String CONF_DUMMY_HOST_PATH = "naivebayes.dataset.dummy_host_path";
	public static final String CONF_FIRST_SESSION = "naivebayes.start.date";
	public static final String CONF_FIRST_SYNTHESIZED_SESSION = "naivebayes.synthesizer.from";
	public static final String CONF_GENERATE_STATIC_DUMMIES = "naivebayes.dataset.generate_static_dummies";
	public static final String CONF_HEADER_PATH = "naivebayes.header.path";
	public static final String CONF_HOSTINDEX_FILE = "naivebayes.hostindex.file";
	public static final String CONF_IGNORE_FREQUENCIES = "naivebayes.dataset.ignore_frequencies";
	public static final String CONF_LAST_SESSION = "naivebayes.end.date";
	public static final String CONF_LAST_SYNTHESIZED_SESSION = "naivebayes.synthesizer.from";
	public static final String CONF_LOGDATA_PATH = "naivebayes.logdata.path";
	public static final String CONF_MATRIX_PATH = "naivebayes.matrix.path";
	public static final String CONF_MAX_REQUESTS_PER_PROFILE = "naivebayes.dataset.max_requests_per_profile";
	public static final String CONF_MIN_HOST_OCCURRENCE_FREQUENCY = "naivebayes.dataset.min_host_occurence_freq";
	public static final String CONF_MIN_REQUESTS_PER_PROFILE = "naivebayes.dataset.min_requests_per_profile";
	public static final String CONF_MINIMUM_DF_OF_HOSTS = "naivebayes.dataset.minimum_df_of_hosts";
	public static final String CONF_NGRAMS_PATH = "naivebayes.ngrams.path";
	public static final String CONF_NGRAMS_SIZE = "naivebayes.ngrams.size";
	public static final String CONF_NGRAMS_ADD_LOWER = "naivebayes.ngrams.preserve_1grams";
	public static final String CONF_NGRAMS_SKIP_AAAA = "naivebayes.ngrams.skip_aaaa";
	public static final String CONF_RANGE_QUERIES_PATH = "naivebayes.range_queries.path";
	public static final String CONF_RANGE_QUERIES_NUMBER_OF_DUMMY_SAMPLES = "naivebayes.dataset.number_of_samples";
	public static final String CONF_NUM_ATTRIBUTES = "naivebayes.num.attributes";
	public static final String CONF_NUM_CLASSES = "naivebayes.num.classes";
	public static final String CONF_NUM_INSTANCES = "naivebayes.num.instances";
	public static final String CONF_NUMBER_OF_DUMMIES_PER_REQUEST = "naivebayes.dataset.number_of_dummies";
	public static final String CONF_NUMBER_OF_SYNTHESIZED_DAYS = "naivebayes.synthesizer.number_of_days";
	public static final String CONF_OFFSET_BETWEEN_TRAINING_AND_TEST = "naivebayes.test.offset";
	public static final String CONF_ONLY_N_MOST_POPULAR_HOSTNAMES = "naivebayes.dataset.only_n_most_popular_hostnames";
	public static final String CONF_OPTIONS = "naivebayes.option";
	public static final String CONF_RANDOM_TRAINING_SESSIONS_SEED = "naivebayes.training.randomizer.seed";
	public static final String CONF_REJECT_INVALID_HOSTNAMES = "naivebayes.dataset.reject_invalid_hostnames";
	public static final String CONF_REJECT_WINDOWS_BROWSER_QUERIES = "naivebayes.dataset.reject_windows_browser_queries";
	public static final String CONF_SESSION_DURATION = "naivebayes.session.time";
	public static final String CONF_SESSION_OFFSET = "naivebayes.session.offset";
	public static final String CONF_SKIP_N_MOST_POPULAR_HOSTNAMES = "naivebayes.dataset.skip_n_most_popular_hostnames";
	public static final String CONF_SYNTHESIZED_DATASET_PATH = "naivebayes.synthesized_dataset.path";
	public static final String CONF_TEST_DATE = "naivebayes.test.date";
	public static final String CONF_TEST_PATH = "naivebayes.test.path";
	public static final String CONF_TEST_DROP_AMBIGUOUS_RESULTS = "naivebayes.test.drop_ambigious_results";
	public static final String CONF_TRAINING_DATE = "naivebayes.training.date";
	public static final String CONF_TRAINING_PATH = "naivebayes.training.path";
	public static final String CONF_TRAINING_SEED = "naivebayes.training.seed";
	public static final String CONF_TRAINING_USER_RATIO = "naivebayes.training.user_ratio";
	public static final String CONF_TRAINING_WITH_LIMITED_DATASET = "naivebayes.training.with_limited_dataset";
	public static final String CONF_USE_DAILY_DOCUMENT_FREQUENCIES = "naivebayes.dataset.use_daily_document_frequencies";
	public static final String CONF_USE_OVERALL_DOCUMENT_FREQUENCIES = "naivebayes.dataset.use_overall_document_frequencies";;
	public static final String CONF_USE_DYNAMIC_REQUEST_RANGE_INDEX = "naivebayes.dataset.use_dynamic_request_range_index";
	public static final String CONF_USE_PRC = "naivebayes.test.use_prc";
	public static final String CONF_USE_RANDOM_TRAINING_SESSIONS = "naivebayes.training.randomizer.sessions";
	public static final String CONF_USE_SYNTHESIZE = "naivebayes.synthesizer.enable";
	public static final String CONF_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_TOP_USERS = "naivebayes.dataset.use_daily_document_frequencies.only_top_users";
	public static final String CONF_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_BOTTOM_USERS = "naivebayes.dataset.use_daily_document_frequencies.only_bottom_users";
	public static final String CONF_DATASET_ONLY_TYPE_A_AND_AAAA = "naivebayes.dataset.only_type_a_and_aaaa";
	public static final String CONF_DATASET_IGNORE_SOPHOSXL = "naivebayes.dataset.ignore_sophosxl";
	public static String CONF_WRITE_HOST_INDEX_MAPPING = "naivebayes.header.write_host_index_mapping";
	public static final String CONF_COUNT_EFFECTIVE_ATTRIBUTES_OPTION = "naivebayes.header.count_effective_attributes_option";
	
	public static final String CONF_NUM_NODES = "cluster.num_nodes";
	public static final String CONF_NUM_CORES = "cluster.num_cores";

	public static final int DEFAULT_NUM_NODES = 12; // HAMBURG 12, REGENSBURG: 7
	public static final int DEFAULT_NUM_CORES = 2; // don't be greedy! for quad
	// cores a value of 2 is
	// safe here

	/**
	 * The size at which the data set files will be split.
	 */
	public static int DATASET_MB_SPLIT = 12 * 1024 * 1024; // 12mb
	public static final boolean DEFAULT_DOWNCASE_HOSTNAMES = false;
	public static final boolean DEFAULT_IGNORE_FREQUENCIES = false;
	public static final int DEFAULT_NGRAMS_SIZE = 1;
	public static final boolean DEFAULT_NGRAMS_PRESERVE_1GRAMS = false;
	public static final boolean DEFAULT_NGRAMS_SKIP_AAAA = false;
	public static final int DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST = 1440;
	public static final int DEFAULT_ONLY_N_MOST_POPULAR_HOSTNAMES = 0;
	public static final boolean DEFAULT_REJECT_INVALID_HOSTNAMES = false;
	public static final boolean DEFAULT_REJECT_WINDOWS_BROWSER_QUERIES = false;

	// EXPERIMENTAL
	public static HashMap<String, Object> defaults = new HashMap<String, Object>();

	/**
	 * The default session duration
	 */
	public static int DEFAULT_SESSION_DURATION = 60 * 24; // 1d
	public static int DEFAULT_SESSION_OFFSET = 0; // offset in minutes from
	// 00:00 o'clock
	public static final int DEFAULT_SKIP_N_MOST_POPULAR_HOSTNAMES = 0;

	public static final boolean DEFAULT_USE_DAILY_DOCUMENT_FREQUENCIES = false;
	public static final boolean DEFAULT_USE_DYNAMIC_REQUEST_RANGE_INDEX = false;
	public static final boolean DEFAULT_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_BOTTOM_USERS = false;
	public static final boolean DEFAULT_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_TOP_USERS = false;
	public static final boolean DEFAULT_USE_OVERALL_DOCUMENT_FREQUENCIES = false;

	/**
	 * A date format.
	 */
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
	/**
	 * The user for hadoop.
	 */
	public static String HADOOP_USER = "hadoop,hadoop";
	static final private Pattern hostnamePattern = Pattern
			.compile("^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])(\\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9]))*$");

	/**
	 * regex used to identify hosts used in queries issued by the Windows
	 * Network Browser (HOSTNAME.uni-regensburg.de)
	 */
	static final private Pattern windowsBrowserQueryPattern = Pattern
			.compile("^([A-Z0-9-]+)\\.uni-regensburg\\.de$");

	/**
	 * regular expressions for hostname validation
	 */
	static final private Pattern ipPattern = Pattern
			.compile("^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");

	/**
	 * The job name prefix.
	 */
	public static String JOB_NAME = "NaiveBayes";
	public static String JOB_NAME_YANG = "Yang";
	public static String JOB_NAME_JACCARD = "Jaccard";
	public static String JOB_NAME_COSIM = "Cosim";

	/**
	 * The Number of Top-Patters per User
	 */
	public static final String CONF_NUM_TOP_PATTERNS = "num.toppattern";
	
	// perclass=true means that num top patterns are drawn from the union of
	//    all instances per day (n_patterns on a user level),
	// otherwise num top patterns are drawn per instance,
	//    i.e. numinstances*numpatterns are drawn per user (n_patterns on an instance level)
	public static final String CONF_TOP_PATTERNS_PER_CLASS = "perclass.toppattern";
	public static final String CONF_GLOBAL_TOP_PATTERNS = "global.toppattern";
	public static final String CONF_CANDIDATEPATTERN_PATH = "candidatepattern.path";
	/**
	 * use same important Patterns for Top-Patterns creation
	 */
	public static final String CONF_NO_SAME_TOPPATTERNS = "same.toppattern";

	/**
	 * The Classifier to use (default is "mnb")
	 */
	public static final String CONF_CLASSIFIER = "classifier";
	public static final String CONF_DATASET_FILTER_METHOD = "method.filter.dataset";
	public static final String CONF_DATASET_FILTER = "filter.dataset";
	/**
	 * Last access time.
	 */
	static long lastTime = System.currentTimeMillis();
	public static final int LOG_ENTRY_INDEX_HOST = 3;

	// Dieses File ist der output von digger.rb, d.h. die Hosts sind bereinigt,
	// d.h. es fehlen einige Hosts
	// public static final String PATH_MOST_POPULAR_HOSTNAMES =
	// "/user/dnsdaten/analysis/most_popular_hostnames_for_linkability";

	public static final int LOG_ENTRY_INDEX_LENGTH = 5;

	// Dieses File enth�lt WIRKLICH alle Hosts in absteigender Reihenfolge -->
	// Erkennungsraten waren damit aber BESSER als bei
	// analysis_requests_per_hostname_wohnheime_lower_a
	// public static final String PATH_MOST_POPULAR_HOSTNAMES =
	// "/user/hive/warehouse/analysis_visitors_per_hostname_wohnheime_lower_a/attempt_201103030925_0312_r_000000_0";

	// Dieses File liegt lokal zum Debuggen
	// public static final String PATH_MOST_POPULAR_HOSTNAMES =
	// "/Users/dh/Repositories/uni-dnsanalyse/most_popular_hostnames_for_linkability";

	public static final int LOG_ENTRY_INDEX_REQUEST_TYPE = 4;

	public static final int LOG_ENTRY_INDEX_TIME_MILLISECONDS = 1;

	public static final int LOG_ENTRY_INDEX_TIME_SECONDS = 0;

	public static final int LOG_ENTRY_INDEX_USER = 2;

	public static final int CONFUSION_MATRIX_CLASS = 0;
	public static final int CONFUSION_MATRIX_NUMBER_OF_INSTANCES_ASSIGNED = 1;
	public static final int CONFUSION_MATRIX_TRUE_POSITIVE_RATE = 2;
	public static final int CONFUSION_MATRIX_FALSE_POSITIVE_RATE = 3;
	public static final int CONFUSION_MATRIX_PRECISION = 4;
	public static final int CONFUSION_MARTIX_FIRST_ASSIGNED_INSTANCE = 5;

	/**
	 * The pseudo index of missing attributes.
	 */
	public static int MISSING_ATTRIBUTE_INDEX = -1;

	// Dieses File enth�lt WIRKLICH alle Hosts in absteigender Reihenfolge
	// public static final String PATH_MOST_POPULAR_HOSTNAMES =
	// "/user/hive/warehouse/analysis_requests_per_hostname_wohnheime_lower_a/attempt_201103030925_0002_r_000000_0";

	// top 100000 hosts
	public static final String PATH_MOST_POPULAR_HOSTNAMES = "/user/hive/warehouse/top_100000_hostnames_dorm/000000_0";

	public static final String PROBABILITY_OF_CLASS_PATH = "probabilityOfClass";

	public static final String PROBABILITY_OF_WORD_GIVEN_CLASS_PATH = "probabilityOfWordGivenClass";

	public static final String PRECISION_PATH = "precision";

	public static final String RECALL_PATH = "recall";

	public static final String INSTANCES_PER_CLASS_PATH = "instancesPerClass";

	public static final String PREDICTED_CLASSES_PATH = "predictedClasses";

	public static final String CROSS_VALIDATION_PATH = "crossValidation";
	public static final String NAMES_PATH = "mobilemeident.names_path";
	public static final int SESSION_COUNT = -2;
	public static final String CONF_INTERINTRA = "interintra";
	public static final String CONF_INTERINTRA_PATH = "interintra.path";
	public static final String CONF_CACHING_DURATION = "caching.duration";
	public static final String CONF_CACHING_SIMULATOR_PATH = "caching.path";
	public static final String CONF_CACHING_SIMULATOR = "caching.simulator";
	public static final String CONF_CACHING_FIXEDDURATION = "fixedduration";
	public static final String CONF_CACHING_WITHINSESSION = "withinsession";
	public static final String CONF_CACHING_SLIDINGWINDOW = "slidingwindow";
	
	public static final boolean DEFAULT_OMNISCIENT_CLASSIFIER = false;
	public static final String CONF_OMNISCIENT_CLASSIFIER = "naivebayes.omniscient_classifier";

	/**
	 * A java runtime.
	 */
	public static Runtime rt = Runtime.getRuntime();
	/**
	 * Time the program started.
	 */
	static long startTime = System.currentTimeMillis();

	// set the time zone to GMT
	static {
		df.setTimeZone(TimeZone.getTimeZone("GMT"));
		defaults.put(CONF_DATASET_USER_WHITELIST_ENABLED, false);
		defaults.put(CONF_DATASET_HOST_WHITELIST_ENABLED, false);
		defaults.put(CONF_DATASET_USER_BLACKLIST_ENABLED, false);
		defaults.put(CONF_DATASET_HOST_BLACKLIST_ENABLED, false);
		defaults.put(CONF_TEST_DROP_AMBIGUOUS_RESULTS, false);
		defaults.put(CONF_CROSS_VALIDATION_SEED, 1);
		defaults.put(CONF_OPEN_WORLD_NUM_TRAINING_INSTANCES, 1000);
		defaults.put(CONF_OPEN_WORLD_NUM_TEST_INSTANCES, 4000);
		defaults.put(CONF_SESSION_DURATION, 1440);
		defaults.put(CONF_INCLUDE_ALL_STATIC_USERS, false);
		defaults.put(CONF_DATASET_IGNORE_SOPHOSXL, false);
		defaults.put(CONF_DATASET_ONLY_TYPE_A_AND_AAAA, false);
		defaults.put(CONF_WRITE_HOST_INDEX_MAPPING, false);
	}

	public static HashMap<String, Boolean> userWhiteList = new HashMap<String, Boolean>();
	public static HashMap<String, Boolean> hostWhiteList = new HashMap<String, Boolean>();
	public static HashMap<String, Boolean> userBlackList = new HashMap<String, Boolean>();
	public static HashMap<String, Boolean> hostBlackList = new HashMap<String, Boolean>();
	

	public static void optimizeSplitSize(Job job) throws IOException {
		optimizeSplitSize(job, 1);
	}

	public static void optimizeSplitSize(Job job, int multipleOfNodes)
			throws IOException {
		long totalSizeOfInputFiles = 0;

		Configuration conf = job.getConfiguration();

		final int numCores = conf.getInt(CONF_NUM_CORES, DEFAULT_NUM_CORES);
		final int numNodes = conf.getInt(CONF_NUM_NODES, DEFAULT_NUM_NODES);

		int numberOfAvailableMapper = numNodes * numCores * multipleOfNodes;

		String dirs = conf.get("mapred.input.dir", "");
		String[] list = StringUtils.split(dirs);

		Path inputPath;
		FileSystem fs = FileSystem.get(job.getConfiguration());

		for (int i = 0; i < list.length; i++) {
			inputPath = new Path(StringUtils.unEscapeString(list[i]));
			for (FileStatus fileStatus : fs.listStatus(inputPath)) {
				totalSizeOfInputFiles += fileStatus.getLen();
			}
		}

		long splitSize = (totalSizeOfInputFiles / numberOfAvailableMapper);

		Util.showStatus("Setting split size to: " + (double) splitSize
				/ (1000 * 1000) + " MB");

		FileInputFormat.setMinInputSplitSize(job, splitSize);
		FileInputFormat.setMaxInputSplitSize(job, splitSize);
	}

	/**
	 * Calculates the probability of an attribute.
	 * 
	 * @param occ
	 *            The occurrence of the attribute
	 * @param wordsPerClass
	 *            The word count in the class the attribute belongs to.
	 * @param numAttributes
	 *            The total number of attributes in the data set.
	 * @return The bayes probability.
	 */
	public static double bayes(double occ, double wordsPerClass,
			int numAttributes) {

		// +1 because of laplace
		return Math.log((occ + 1) / (wordsPerClass + numAttributes));
	}

	public static String[] splitAndReverse(String string) {
		List<String> list = Arrays.asList(Util.fastSplit(string, '.'));

		Collections.reverse(list);

		return list.toArray(new String[list.size()]);
	}

	public static float reverseBayes(float p, int wordsPerClass,
			int numAttributes) {
		return (float) ((Math.exp(p) * (wordsPerClass + numAttributes)) - 1);
	}

	public static double bayesPrH(double docsPerClass, int numInstances,
			int numClasses) {
		/***
		 * WEKA_CODE: calculating Pr(H) NOTE: Laplace estimator introduced in
		 * case a class does not get mentioned in the set of training instances
		 * final double numDocs = instances.sumOfWeights() + m_numClasses; <--
		 * WIESO HIER numInstances + numClasses !??! m_probOfClass = new
		 * double[m_numClasses]; for (int h = 0; h < m_numClasses; h++)
		 * m_probOfClass[h] = (double) (docsPerClass[h] + 1) / numDocs;
		 */
		// +1 because of laplace
		final int numDocs = numInstances + numClasses; // versteh ich nicht...

		return (docsPerClass + 1) / numDocs;
	}

	/**
	 * Deletes empty files in the specified directory.
	 * 
	 * @param fs
	 *            A <i>FileSystem</i>.
	 * @param path
	 *            The path.
	 * @throws IOException
	 */
	public static void deleteEmptyFiles(FileSystem fs, Path path)
			throws IOException {
		for (FileStatus fileStatus : fs.listStatus(path)) {
			if (!fileStatus.isDir() && (fileStatus.getLen() == 0)) {
				fs.delete(fileStatus.getPath(), false);
			}
		}
	}

	/**
	 * Formats a double.
	 * 
	 * @param d
	 *            A double.
	 * @return The formatted <i>String</i>.
	 */
	public static String format(double d) {
		NumberFormat nf = NumberFormat.getInstance(Locale.ENGLISH);
		nf.setMinimumFractionDigits(6);
		nf.setMaximumFractionDigits(6);
		return nf.format(d);
	}

	/**
	 * Formats a date.
	 * 
	 * @param date
	 *            The date.
	 * @return The formatted <i>String</i>.
	 */
	public static String formatDate(Date date) {
		return df.format(date);
	}

	/**
	 * Formats a <i>Calendar<i> object according to the session durations.
	 * 
	 * @param cal
	 *            The <i>Calendar</i>
	 * @param sessionDuration
	 *            The session duration in minutes.
	 * @param offset
	 *            The offset for session start counting from 00:00:00 of each
	 *            day in minutes. Sessions will start at midnight (UTC) for an
	 *            offset of 0.
	 * 
	 * @return The formatted <i>String</i>.
	 * 
	 *         Exemplatory explanation:
	 * 
	 *         M�gliche Session Beginne:
	 * 
	 *         alt: 1971-01-01-00-00 = 31536000
	 * 
	 *         neu: 1971-01-01-04-00 = 31550400
	 * 
	 *         offset = 31550400-31536000 = 14400 s = 240 min Session Length =
	 *         1440 min = 86400 s
	 * 
	 * 
	 * 
	 *         Jetzt kommt eine Zeile mit 31570400 (09:33:20 AM UTC).
	 * 
	 *         Abzuziehen sind: 526173 % 1440 = 573 min
	 * 
	 *         Ergebnis: = 526173 - 573 = 525600 min = 525600 * 60 = 31536000
	 *         passt
	 * 
	 *         Wenn man nun nicht 573, sondern 573 - offset(240) abziehen
	 *         w�rde, also 333, dann:
	 * 
	 *         526173 - 333 = 525840 min = 525840 * 60 = 31550400 -> passt
	 * 
	 *         Aber: was ist um 00:10:00 Uhr (31536600 sek), d.h. 525610 min
	 * 
	 *         Abzuziehen sind: 525610 % 1440 = 10
	 * 
	 *         nach obiger Regel jedoch "10 - offset", das w�re falsch!
	 * 
	 *         Richtig ist in diesem Fall: (10 - offset) + 1440 sind abzuziehen
	 *         also 10 - 240 = -230 + 1440 = 1210
	 * 
	 *         525610 - 1210 = 524400 min = 524400 * 60 = 31464000 = 31.12.1970
	 *         04:00:00 -> passt
	 * 
	 *         D.h. wenn das abzuziehende Ergebnis < 0 ist, muss Sessionlength
	 *         hinzuaddiert werden
	 */
	public static String formatDateAccordingToSessionDuration(Calendar cal,
			int sessionDuration, int offset) {
		long time = cal.getTimeInMillis();
		long minutes = (time / (1000 * 60));

		// calculate how much we need to subtract to get into the last session
		int diff = (int) (minutes % sessionDuration);

		// take into account the offset
		diff = diff - offset;
		if (diff < 0)
			diff += sessionDuration;

		time = (minutes - diff) * 1000 * 60;

		return df.format(new Date(time));
	}

	/**
	 * Parses a string and returns a <i>Calendar<i> object.
	 * 
	 * @param date
	 *            The date string.
	 * @return The <i>Calendar<i> object representing the parsed <i>String<i>.
	 * @throws ParseException
	 */
	public static Calendar getCalendarFromString(String date)
			throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(df.parse(date));

		return cal;
	}

	public static CompactCharSequence getHash(MessageDigest digest, String key)
			throws IOException {
		try {
			byte[] hostHashBytes = new byte[8];
			System.arraycopy(digest.digest(key.getBytes()), 0, hostHashBytes,
					0, 8);
			return new CompactCharSequence(hostHashBytes);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

	}

	/**
	 * Print Hex String representation of a byte array
	 * 
	 * @param b
	 *            The byte array
	 * @return String
	 * @throws Exception
	 */
	public static String getHexString(byte[] b) {
		String result = "";
		for (int i = 0; i < b.length; i++) {
			result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
		}
		return result;
	}

	/**
	 * Returns a hashmap of all hosts with a document frequency above the
	 * threshold
	 * 
	 * @obsolete
	 * @param context
	 * @param threshold
	 * @return
	 * @throws IOException
	 */
	public static TLongFloatHashMap getHostsWithDocumentFrequencies(
			Configuration conf, float threshold) throws IOException {
		String line = null;
		String rr[] = null;

		TLongFloatHashMap map = new TLongFloatHashMap();

		Path file = null;

		Path trainingPath = new Path(
				conf.get(Util.CONF_DOCUMENT_FREQUENCY_PATH) + "/"
						+ conf.get(Util.CONF_SESSION_DURATION) + "/"
						+ conf.get(Util.CONF_OPTIONS) + "/"
						+ conf.get(Util.CONF_TRAINING_DATE));
		FileSystem fs = FileSystem.get(conf);

		// String lastClass = null;
		for (FileStatus fileStatus : fs.listStatus(trainingPath)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();

			// indexOfHost \t
			// "proportion of documents containing this host <double>"
			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			while (true) {
				line = reader.readLine();

				if (line == null) {
					break;
				}

				rr = line.split("\t");

				long indexOfHost = Long.parseLong(rr[0]);
				float documentFrequency = Float.parseFloat(rr[1]);

				if (documentFrequency > threshold) {
					map.put(indexOfHost, documentFrequency);
				}
			}
		}

		return map;
	}

	/**
	 * Returns a hashmap of all users with their instances (dates) that meet the
	 * specified requirement
	 * 
	 * @param context
	 * @param minNumberOfInstancesPerClass
	 *            only classes which meet this requirement are returned
	 * @return
	 * @throws IOException
	 */
	public static Map<String, Set<String>> getOccurenceMatrix(
			Configuration conf, final int minNumberOfInstancesPerClass)
			throws IOException {
		String line = null;
		String rr[] = null;

		Map<String, Set<String>> map = new HashMap<String, Set<String>>();

		Path file = null;

		Path occurenceMatrixPath = new Path(conf.get(Util.CONF_HEADER_PATH)
				+ "/" + DataSetHeader.OCCURRENCE_MATRIX_DIRECTORY);

		FileSystem fs = FileSystem.get(conf);

		// String lastClass = null;
		String[] dates;
		String user;
		String datesList;

		for (FileStatus fileStatus : fs.listStatus(occurenceMatrixPath)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();

			// user \t date1,date2,...
			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			while (true) {
				line = reader.readLine();

				if (line == null) {
					break;
				}

				rr = Util.veryFastSplit(line, '\t', 2);

				user = rr[0];
				datesList = rr[1];
				dates = Util.fastSplit(datesList, ',');

				// skip this user if he was not active on enough dates
				if (dates.length < minNumberOfInstancesPerClass)
					continue;

				// otherwise add the user together with its instances to the map
				Set<String> datesSet = new HashSet<String>(Arrays.asList(dates));
				map.put(user, datesSet);
			}
		}
		return map;
	}

	/**
	 * Returns array of paths within the log file directory
	 * 
	 * @param fs
	 *            A <i>FileSystem</i>.
	 * @param path
	 *            The path.
	 * @throws IOException
	 */
	public static ArrayList<Path> getInputDirectories(FileSystem fs, Path path)
			throws IOException {
		ArrayList<Path> inputPaths = new ArrayList<Path>();

		// for (FileStatus fileStatus : fs.globStatus(new
		// Path(path.getName().concat("/*")))) {
		for (FileStatus fileStatus : fs.listStatus(path)) {
			if (fileStatus.isDir()) {
				inputPaths.add(fileStatus.getPath());
			}
		}
		return inputPaths;
	}

	public static long getLongFromFirst8BytesHash(MessageDigest digest,
			String text) throws IOException {
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(
					digest.digest(text.getBytes()));
			DataInputStream dis = new DataInputStream(bis);

			long l = dis.readLong();
			l = dis.readLong();

			dis.close();
			bis.close();

			return l;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

	}

	/**
	 * Prints out detailed memory information.
	 * 
	 * @param clazz
	 *            The class that called it.
	 * @return Used memory.
	 */
	public static double getMemoryInfo(Class<?> clazz) {
		NumberFormat nf = NumberFormat.getInstance();
		Log log = LogFactory.getLog(clazz);

		log.info("GETTING MEMORY USAGE...");
		double fm = (double) rt.freeMemory() / (1024 * 1024 * 1024);
		double tm = (double) rt.totalMemory() / (1024 * 1024 * 1024);
		double mm = (double) rt.maxMemory() / (1024 * 1024 * 1024);
		log.info("   Free memory = " + nf.format(fm) + "gb");
		log.info("   Total memory = " + nf.format(tm) + "gb");
		log.info("   Maximum memory = " + nf.format(mm) + "gb");
		log.info("   Memory used = " + nf.format(tm - fm) + "gb");
		return tm - fm;
	}

	/**
	 * Return the 2nd level domain name for a given host or null if not
	 * applicable, e.g. google.com for images.google.com google.de for google.de
	 * null for localhost
	 * 
	 * Note: Host MUST be lowercase!
	 * 
	 * @param host
	 *            The hostname for which the 2nd level domain should be returned
	 * @return 2nd level domain or null if not applicable
	 */
	public static String getSecondLevelDomain(String host) {
		String secondLevelDomain = null;

		if (host.endsWith("in-addr.arpa") || host.endsWith("ip6.arpa")) {
			return null;
		}

		if (Util.isValidIP(host)) {
			return null;
		}

		int indexOfLastDot = host.lastIndexOf('.');
		int indexOfSecondLastDot = -1;

		if (indexOfLastDot > 0) {
			indexOfSecondLastDot = host.lastIndexOf('.', indexOfLastDot - 1);
			secondLevelDomain = host.substring(indexOfSecondLastDot + 1);
		}
		return secondLevelDomain;
	}

	public static byte[] hexStringToByteArray(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character
					.digit(s.charAt(i + 1), 16));
		}
		return data;
	}

	public static boolean isInArray(int[] haystack, int needle) {
		for (int hay : haystack) {
			if (hay == needle) {
				return true;
			}
		}

		return false;
	}

	public static boolean isValidHostname(String line) {
		return hostnamePattern.matcher(line).matches();
	}

	public static boolean isWindowsBrowserQuery(String host) {
		return windowsBrowserQueryPattern.matcher(host).matches();
	}

	public static boolean isValidIP(String line) {
		return ipPattern.matcher(line).matches();
	}

	public static void log(Class<?> clazz, String message) {
		LogFactory.getLog(clazz).info(message);
	}

	/**
	 * Write a ping! to the logger belonging to a specified class.
	 * 
	 * @param clazz
	 *            A class.
	 */
	public static void logPing(Class<?> clazz) {
		LogFactory.getLog(clazz).info("ping!");
	}

	/**
	 * Reports progress to the mapper.
	 * 
	 * @param context
	 *            A <i>MapContext<i>.
	 * @param clazz
	 *            The class that wants to report progress.
	 */
	public static void ping(MapContext<?, ?, ?, ?> context, Class<?> clazz) {
		final long currtime = System.currentTimeMillis();
		if (currtime - lastTime > 10000) {
			context.progress();
			lastTime = currtime;
			logPing(clazz);
		}
	}

	/**
	 * Reports progress to the reducer.
	 * 
	 * @param context
	 *            A <i>ReduceContext<i>.
	 * @param clazz
	 *            The class that wants to report progress.
	 */
	public static void ping(ReduceContext<?, ?, ?, ?> context, Class<?> clazz) {
		final long currtime = System.currentTimeMillis();
		if (currtime - lastTime > 10000) {
			context.progress();
			lastTime = currtime;
			logPing(clazz);
		}
	}

	/**
	 * Reports progress to a task.
	 * 
	 * @param context
	 *            A <i>TaskAttemptContext<i>.
	 * @param clazz
	 *            The class that wants to report progress.
	 */
	public static void ping(TaskAttemptContext context, Class<?> clazz) {
		final long currtime = System.currentTimeMillis();
		if (currtime - lastTime > 10000) {
			context.progress();
			lastTime = currtime;
			logPing(clazz);
		}
	}

	/**
	 * Read file with Top N hosts, which can be used to simulate the influence
	 * of prefetching them.
	 * 
	 * @param numberOfHosts
	 *            the number of hosts to read
	 * @param pathToTopHostFile
	 * @param a
	 *            map context
	 * 
	 * @return set with read hostnames
	 */
	public static Set<String> readTopNHostsFromFile(int numberOfHosts,
			String pathToTopHostFile, Configuration conf) throws IOException {

		System.out.println("Reading top " + numberOfHosts + " hostnames from "
				+ pathToTopHostFile);

		Set<String> hostSet = new HashSet<String>(numberOfHosts);

		FileSystem fs = FileSystem.get(conf);

		FSDataInputStream stream = fs.open(new Path(pathToTopHostFile));
		LineNumberReader lnr = new LineNumberReader(new InputStreamReader(
				stream));

		String line;
		String lineElements[];

		while (((line = lnr.readLine()) != null)
				&& (lnr.getLineNumber() <= numberOfHosts)) {
			lineElements = line.split("\t");
			// expected file format: hostname ttl replysize
			hostSet.add(lineElements[0]);
		}

		lnr.close();
		stream.close();

		return hostSet;
	}

	public static String[] readDummyHosts(TaskAttemptContext context,
			String confDummyHostPath, Class<?> clazz) throws IOException {
		Path path = new Path(confDummyHostPath);
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Random rand = new Random();

		// hard coded 1000000 elements because by default the dummy host file
		// contains that many domains
		String[] dummyPool = new String[1000000];

		// source:
		// http://stackoverflow.com/questions/106179/regular-expression-to-match-hostname-or-ip-address

		FSDataInputStream in = fs.open(path);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		int i = 0;
		while (true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}

			if (Util.isValidIP(line)) {
				// LogFactory.getLog(clazz).info("skipping IP address: " +
				// line);
				continue;
			}

			if (!Util.isValidHostname(line)) {
				// LogFactory.getLog(clazz).info("skipping invalid host: " +
				// line);
				continue;
			}
			dummyPool[i++] = line;
			if (i % 100000 == 0) {
				Util.ping(context, GenerateDataSetReducer.class);
			}
		}

		int numberOfSamples = conf.getInt(
				Util.CONF_RANGE_QUERIES_NUMBER_OF_DUMMY_SAMPLES, 100000);
		// pick the requested number of dummies
		String[] dummySamples = new String[numberOfSamples];

		// we use a constant seed for repeatability
		rand.setSeed(1);

		for (i = 0; i < numberOfSamples; i++) {
			int r = rand.nextInt(dummyPool.length);

			if (dummyPool[r] == null) {
				i--;
				continue;
			} // host has already been drawn; skip!

			dummySamples[i] = dummyPool[r];
			dummyPool[r] = null;
		}

		return dummySamples;
	}

	/*
	 * public String[] getUserWhiteList(Path path) { ArrayList<String> list =
	 * new ArrayList }
	 */

	public static void populateBlackOrWhiteList(Configuration conf,
			HashMap<String, Boolean> whiteList, String path) {
		log(Util.class, "reading black or white list " + path);
		int i = 0;
		try {
			FileSystem fs = FileSystem.get(conf);
			Path file = new Path(conf.get(path));

			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			String line = null;
			while ((line = reader.readLine()) != null) {
				whiteList.put(line, true);
				i++;
			}

			reader.close();
			in.close();
		} catch (IOException ex) {
			log(Util.class,
					"Error reading black or whitelist. Exception is below. See STDOUT for stack trace.");
			log(Util.class, ex.getMessage());
			ex.printStackTrace();
			System.exit(1);

		}
		log(Util.class, "done reading the list");
	}

	/**
	 * This utility function determines whether a log entry should be used or
	 * discarded. This function needs to be modified if you want to extend or
	 * reduce the data set.
	 * 
	 * @param user
	 *            The user who sent the query.
	 * @param host
	 *            The host that was queried.
	 * @param type
	 *            The type of dns query.
	 * 
	 * @return True if the log entry is used, false otherwise.
	 */
	public static boolean sanitize(TaskAttemptContext context, String user,
			String host, String type) {

		Configuration conf = context.getConfiguration();

		// discard all queries that are not A or AAAA
		if (Util.isEnabled(conf, Util.CONF_DATASET_ONLY_TYPE_A_AND_AAAA)) {
			if (!type.equals("A") && !type.equals("AAAA")) {
				return false;
			}
		}

		/*
		 * 0bd8d89d40fbae9653ca9747c2310506 is a bad bad bot, maybe exclude him?
		 * if(classLabel.equals("0bd8d89d40fbae9653ca9747c2310506")) { return
		 * false; }
		 */

		// skip all sophos hosts to bring down nmbr of hosts from 80 million to
		// 40 million
		if (Util.isEnabled(conf, Util.CONF_DATASET_IGNORE_SOPHOSXL)) {
			if (host.endsWith(".sophosxl.com")) {
				return false;
			}
		}

		if (Util.isEnabled(conf, Util.CONF_DATASET_USER_WHITELIST_ENABLED)) {
			if (Util.userWhiteList.size() == 0) {
				populateBlackOrWhiteList(conf, userWhiteList,
						Util.CONF_DATASET_USER_WHITELIST_PATH);
			}

			if (!userWhiteList.containsKey(user)) {
				return false;
			}
		}

		if (Util.isEnabled(conf, Util.CONF_DATASET_HOST_WHITELIST_ENABLED)) {
			if (Util.hostWhiteList.size() == 0) {
				populateBlackOrWhiteList(conf, hostWhiteList,
						Util.CONF_DATASET_HOST_WHITELIST_PATH);
			}

			if (!hostWhiteList.containsKey(host)) {
				return false;
			}
		}

		if (Util.isEnabled(conf, Util.CONF_DATASET_USER_BLACKLIST_ENABLED)) {
			if (Util.userBlackList.size() == 0) {
				populateBlackOrWhiteList(conf, userBlackList,
						Util.CONF_DATASET_USER_BLACKLIST_PATH);
			}

			if (userBlackList.containsKey(user)) {
				return false;
			}
		}

		if (Util.isEnabled(conf, Util.CONF_DATASET_HOST_BLACKLIST_ENABLED)) {
			if (Util.hostBlackList.size() == 0) {
				populateBlackOrWhiteList(conf, hostBlackList,
						Util.CONF_DATASET_HOST_BLACKLIST_PATH);
			}

			if (hostBlackList.containsKey(host)) {
				// DEBUG
				// log(Util.class, "skipping host... " + host);
				return false;
			}
		}

		// if the user does not contain a "_", it has a static ip, but is not
		// from the dorm network
		if (!user.contains("_")) {
			return Util.isEnabled(conf, Util.CONF_INCLUDE_ALL_STATIC_USERS);
		}

		// include dormitories
		if (user.contains("_1") || user.contains("_2")) {
			return true;
		}

		// discard everything else
		return false;
	}

	// EXPERIMENTAL
	public static Object getDefault(String property) {
		return defaults.get(property);
	}

	// EXPERIMENTAL
	public static boolean isEnabled(Configuration conf, String property) {
		return conf.getBoolean(property, (Boolean) defaults.get(property));
	}

	// EXPERIMANTAL
	public static int getInt(Configuration conf, String property)
			throws Exception {
		try {
			return conf.getInt(property, (Integer) defaults.get(property));
		} catch (Exception ex) {
			throw new Exception(
					"Default value for "
							+ property
							+ " not set! Please add a default value to the Util.defaults hash map!");
		}
	}

	/**
	 * Print the current status to stdout.
	 * 
	 * @param status
	 *            The status.
	 */
	public static void showStatus(Object status) {
		System.out.println("@"
				+ ((System.currentTimeMillis() - startTime) / 1000)
				+ "s | "
				+ (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
						.freeMemory()) / (1000 * 1000) + " mb | " + status);
	}

	public static void writeInfoFile(Configuration conf, FileSystem fs,
			String basePath, String[] args) throws IOException {
		FSDataOutputStream fos = fs.create(new Path(basePath + "/info/"
				+ System.currentTimeMillis()));

		for (String arg : args) {
			fos.writeBytes(arg + " ");
		}
		fos.write('\n');

		Entry<String, String> entry;
		Iterator<Entry<String, String>> it = conf.iterator();

		while (it.hasNext()) {
			entry = it.next();

			if (entry.getKey().startsWith("naivebayes")) {
				fos.writeBytes(entry.getKey() + "=" + entry.getValue() + "\n");
			}
		}

		fos.close();
	}

	public static String[] veryFastSplit(String line, char split, int numEntries) {
		String[] entries = new String[numEntries];
		int wordCount = 0;
		int i = 0;
		int j = line.indexOf(split); // First substring
		while (j >= 0) {
			entries[wordCount++] = line.substring(i, j);
			i = j + 1;
			j = line.indexOf(split, i); // Rest of substrings
		}
		entries[wordCount++] = line.substring(i); // Last substring
		return entries;
	}

	// QUELLE:
	// http://www.coderanch.com/t/509835/java/java/Writing-faster-String-split
	public static String[] fastSplit(String line, char split) {
		String[] temp = new String[line.length() / 2];
		int wordCount = 0;
		int i = 0;
		int j = line.indexOf(split); // First substring
		while (j >= 0) {
			temp[wordCount++] = line.substring(i, j);
			i = j + 1;
			j = line.indexOf(split, i); // Rest of substrings
		}
		temp[wordCount++] = line.substring(i); // Last substring
		String[] result = new String[wordCount];
		System.arraycopy(temp, 0, result, 0, wordCount);
		return result;
	}// end fastSplit

	public static void passthrough(String cmd) throws Exception {
		Process proc = Runtime.getRuntime().exec(cmd);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				proc.getInputStream()));
		String u;
		do {
			u = reader.readLine();
			if (u == null)
				break;

			System.out.println(u);
		} while (true);

		reader.close();
		proc.destroy();
	}

	/**
	 * Verkettet alle Elemente in einem Array (o.a.) zu einem String und
	 * verwendet den Delimiter zwischen zwei Elementen.
	 * 
	 * @param s
	 * @param delimiter
	 * @return
	 */
	public static String joinArrayToString(List<? extends CharSequence> s,
			String delimiter) {
		int capacity = 0;
		int delimLength = delimiter.length();
		Iterator<? extends CharSequence> iter = s.iterator();
		if (iter.hasNext()) {
			capacity += iter.next().length() + delimLength;
		}

		StringBuilder buffer = new StringBuilder(capacity);
		iter = s.iterator();
		if (iter.hasNext()) {
			buffer.append(iter.next());
			while (iter.hasNext()) {
				buffer.append(delimiter);
				buffer.append(iter.next());
			}
		}
		return buffer.toString();
	}

	public static <T> List<T> getRandomSubList(List<T> input, int subsetSize,
			Random r) {
		int inputSize = input.size();
		for (int i = 0; i < subsetSize; i++) {
			int indexToSwap = i + r.nextInt(inputSize - i);
			T temp = input.get(i);
			input.set(i, input.get(indexToSwap));
			input.set(indexToSwap, temp);
		}
		return input.subList(0, subsetSize);
	}

	public static Map<String, Set<String>> selectUsersAndSessionsRandomly(
			Map<String, Set<String>> occurrenceMatrix, int numUsers,
			int numSessions, long seed) {

		Random r = new Random(seed);
		List<String> users = new ArrayList<String>(occurrenceMatrix.keySet());
		Map<String, Set<String>> selectedUsersAndSessions = new HashMap<String, Set<String>>();

		List<String> selectedUsers = getRandomSubList(users, numUsers, r);

		for (String user : selectedUsers) {
			List<String> dates = new ArrayList<String>(
					occurrenceMatrix.get(user));
			List<String> selectedDates = getRandomSubList(dates, numSessions, r);
			selectedUsersAndSessions.put(user, new HashSet<String>(
					selectedDates));
		}
		return selectedUsersAndSessions;
	}

	/**
	 * Needed to rectify the interpretation of timestamp values in calendar
	 * objects. Background: timestamps in our log files are meant to be in local
	 * time, but are interpreted as UTC when setting the calendar with
	 * setTimeInMillis. This method adjusts the calendar by the offset between
	 * local time and UTC so that it actually reflects the correct time.
	 * 
	 * IMPORTANT: This method needs to be called WHENEVER a calendar has been
	 * instantiated with setTimeInMillis
	 * 
	 * @param cal
	 */
	public static void adjustCalendarToLocalTime(Calendar cal) {
		Date date = cal.getTime();

		TimeZone tz = TimeZone.getTimeZone("CET");
		long msFromEpochGmt = date.getTime(); // gets the ms from 1970 in UTC
		// (as set with
		// setTimeInMillis)

		// gives you the current offset in ms from GMT at the date
		// msFromEpochGmt
		int offsetFromUTC = tz.getOffset(msFromEpochGmt);

		cal.setTimeInMillis(msFromEpochGmt + offsetFromUTC);
	}

	public static HashMap<String, TIntArrayList> readCandidatePattern(
			MapContext context) throws Exception {
		HashMap<String, TIntArrayList> candidateMap = new HashMap<String, TIntArrayList>();
		TIntHashSet candidatePatternSet = new TIntHashSet();
		String line = null;
		String rr[] = null;
		Path file = null;
		Configuration conf = context.getConfiguration();
		Calendar firstSession = Util.getCalendarFromString(conf
				.get(Util.CONF_FIRST_SESSION));
		Calendar lastSession = Util.getCalendarFromString(conf
				.get(Util.CONF_LAST_SESSION));
		Calendar currentSession = (Calendar) firstSession.clone();

		String option = conf.get(Util.CONF_OPTIONS);
		Path path = new Path(conf.get(Util.CONF_CANDIDATEPATTERN_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");

		FileSystem fs = FileSystem.get(conf);
		String date = "";

		// for each candidatePattern file
		for (FileStatus fileStatus : fs.listStatus(path)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();
			String s = file.getName();
			if (s.startsWith("part-r-")) {
				continue;
			}
			// for the global-view on the CandidatePatterns date will be
			// "global-candidate-patterns"
			date = s.split("-r")[0];
			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			int i = 0;

			// new InputFile => clear Candidate-Patterns
			candidatePatternSet.clear();

			while (true) {
				line = reader.readLine();

				if (i++ % 10000 == 0) {
					Util.ping(context, Util.class);
					i = 0;
				}
				if (line == null) {
					break;
				}

				rr = line.split("\t");

				// rr[0] user
				// rr[1] hostID

				// the user isn't accually needed for anything...
				int hostID = Integer.parseInt(rr[1]);
				candidatePatternSet.add(hostID);
			}
			TIntArrayList newList = new TIntArrayList(
					candidatePatternSet.toArray());

			if (conf.getBoolean(Util.CONF_GLOBAL_TOP_PATTERNS, false)) {
				while (currentSession.compareTo(lastSession) <= 0) {
					String outputdate = formatDate(currentSession.getTime());

					if (!candidateMap.containsKey(outputdate)) {
						candidateMap.put(outputdate, new TIntArrayList());
					}
					TIntArrayList existingPatterns = candidateMap
							.get(outputdate);
					existingPatterns.addAll(newList);
					// wild hack to make sure that hosts are not inserted twice
					// into
					// existingPatterns
					// (can happen due to multiple files)
					TIntHashSet uniqueHostIds = new TIntHashSet(
							existingPatterns);
					existingPatterns.clear();
					existingPatterns.addAll(uniqueHostIds);
					existingPatterns.sort();

					// jump to the next session
					currentSession.add(Calendar.MINUTE, conf.getInt(
							Util.CONF_SESSION_DURATION,
							Util.DEFAULT_SESSION_DURATION));
				}
			}

			else {
				if (!candidateMap.containsKey(date)) {
					candidateMap.put(date, new TIntArrayList());
				}
				TIntArrayList existingPatterns = candidateMap.get(date);
				existingPatterns.addAll(newList);
				// wild hack to make sure that hosts are not inserted twice into
				// existingPatterns
				// (can happen due to multiple files)
				TIntHashSet uniqueHostIds = new TIntHashSet(existingPatterns);
				existingPatterns.clear();
				existingPatterns.addAll(uniqueHostIds);
				existingPatterns.sort();
			}
		}

		line = null;
		rr = null;
		return candidateMap;

	}

	public static HashMap<String, TIntArrayList> readCandidatePattern(
			ReduceContext context) throws Exception {
		HashMap<String, TIntArrayList> candidateMap = new HashMap<String, TIntArrayList>();
		TIntHashSet candidatePatternSet = new TIntHashSet();
		String line = null;
		String rr[] = null;
		Path file = null;
		Configuration conf = context.getConfiguration();
		Calendar firstSession = Util.getCalendarFromString(conf
				.get(Util.CONF_FIRST_SESSION));
		Calendar lastSession = Util.getCalendarFromString(conf
				.get(Util.CONF_LAST_SESSION));
		Calendar currentSession = (Calendar) firstSession.clone();

		String option = conf.get(Util.CONF_OPTIONS);
		// Path path = new Path(conf.get(Util.CONF_BASE_PATH) +
		// "/candidatepattern" + "/"
		// + conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");
		Path path = new Path(conf.get(Util.CONF_CANDIDATEPATTERN_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/" + option + "/");

		FileSystem fs = FileSystem.get(conf);
		String date = "";

		// for each candidatePattern file
		for (FileStatus fileStatus : fs.listStatus(path)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();
			String s = file.getName();
			if (s.startsWith("part-r-")) {
				continue;
			}
			// for the global-view on the CandidatePatterns date will be
			// "global-candidate-patterns"
			date = s.split("-r")[0];
			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			int i = 0;

			// new InputFile => clear Candidate-Patterns
			candidatePatternSet.clear();

			while (true) {
				line = reader.readLine();

				if (i++ % 10000 == 0) {
					Util.ping(context, Util.class);
					i = 0;
				}
				if (line == null) {
					break;
				}

				rr = line.split("\t");

				// rr[0] user
				// rr[1] hostID

				// the user isn't accually needed for anything...
				int hostID = Integer.parseInt(rr[1]);
				candidatePatternSet.add(hostID);
			}
			TIntArrayList newList = new TIntArrayList(
					candidatePatternSet.toArray());

			if (conf.getBoolean(Util.CONF_GLOBAL_TOP_PATTERNS, false)) {
				while (currentSession.compareTo(lastSession) <= 0) {
					String outputdate = formatDate(currentSession.getTime());

					if (!candidateMap.containsKey(outputdate)) {
						candidateMap.put(outputdate, new TIntArrayList());
					}
					TIntArrayList existingPatterns = candidateMap
							.get(outputdate);
					existingPatterns.addAll(newList);
					// wild hack to make sure that hosts are not inserted twice
					// into
					// existingPatterns
					// (can happen due to multiple files)
					TIntHashSet uniqueHostIds = new TIntHashSet(
							existingPatterns);
					existingPatterns.clear();
					existingPatterns.addAll(uniqueHostIds);
					existingPatterns.sort();

					// jump to the next session
					currentSession.add(Calendar.MINUTE, conf.getInt(
							Util.CONF_SESSION_DURATION,
							Util.DEFAULT_SESSION_DURATION));
				}
			}

			else {
				if (!candidateMap.containsKey(date)) {
					candidateMap.put(date, new TIntArrayList());
				}
				TIntArrayList existingPatterns = candidateMap.get(date);
				existingPatterns.addAll(newList);
				// wild hack to make sure that hosts are not inserted twice into
				// existingPatterns
				// (can happen due to multiple files)
				TIntHashSet uniqueHostIds = new TIntHashSet(existingPatterns);
				existingPatterns.clear();
				existingPatterns.addAll(uniqueHostIds);
				existingPatterns.sort();
			}
		}

		line = null;
		rr = null;
		return candidateMap;

	}

	/**
	 * Checks data directories "training" and "test" for the given session
	 * slots. Returns true if training AND test data is available.
	 * 
	 * @param trainingSession
	 * @param testSession
	 * @return
	 * @throws IOException
	 */
	public static boolean isDataAvailableForTrainingAndTest(Configuration conf,
			Calendar trainingSession, Calendar testSession) throws IOException {

		Path datasetPath = new Path(conf.get(Util.CONF_DATASET_PATH));

		Path trainingPath = new Path(datasetPath + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/"
				+ conf.get(Util.CONF_OPTIONS) + "/"
				+ Util.formatDate(trainingSession.getTime()));
		Path testPath = new Path(datasetPath + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/"
				+ conf.get(Util.CONF_OPTIONS) + "/"
				+ Util.formatDate(testSession.getTime()));

		FileSystem fs = FileSystem.get(conf);
		return (fs.exists(trainingPath) && fs.exists(testPath));
	}
}
