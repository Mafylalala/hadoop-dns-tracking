package de.sec.dns.dataset;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import de.sec.dns.util.Util;

/**
 * This class represent the header of a dataset.
 * 
 * @author Christian Banse
 */
public class DataSetHeader {
	/**
	 * The subdirectory in which the document frequencies for all hosts for each
	 * day are stored
	 */
	public static String DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY = "dailyDocumentFrequencyIndex";

	/**
	 * The subdirectory in which the overall document frequencies for all hosts
	 * are stoed
	 */
	public static String OVERALL_DOCUMENT_FREQUENCY_INDEX_DIRECTORY = "overallDocumentFrequencyIndex";

	/**
	 * The proportion of top and bottom users to drop according to requests per
	 * session
	 */
	public static float DROP_USER_THRESHOLD = 0.05f;

	/**
	 * The subdirectory in which the host index is stored.
	 */
	public static String HOST_INDEX_DIRECTORY = "hostIndex";

	/**
	 * The file which contains metadata about the dataset.
	 */
	public static String META_DATA_FILE = "metadata";
	
	/**
	 * The file which contains metadata about the caching simulation (cf. CachingTool)
	 */
	public static String SIMULATE_CACHING_METADATA_FILE = "metadataSimulateCaching";
	
	
	public static String N_GRAM_INDEX_DIRECTORY = "nGramIndex";

	/**
	 * The subdirectory in which the number of instances per day is stored
	 */
	public static String NUM_INSTANCES_DIRECTORY = "numInstances";

	/**
	 * The subdirectory in which the acceptable number of requests per session
	 * and day index is stored
	 */
	public static String REQUEST_RANGE_INDEX_DIRECTORY = "requestRangeIndex";

	/**
	 * The subdirectory in which the number of instances per day is stored
	 */
	public static String OCCURRENCE_MATRIX_DIRECTORY = "occurrenceMatrix";

	/**
	 * The serial version id.
	 */
	private static final long serialVersionUID = -303368371750889768L;

	/**
	 * The subdirectory in which the user index is stored.
	 */
	public static String USER_INDEX_DIRECTORY = "userIndex";

	public static String HOST_INDEX_MAPPING_DIRECTORY = "hostIndexMapping";

	public static HashMap<String, Integer> getNumInstances(Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		ArrayList<BufferedReader> fileReaders = new ArrayList<BufferedReader>();
		HashMap<String, Integer> numInstances = new HashMap<String, Integer>();
		String line = null;
		String[] rr = null;

		for (FileStatus s : fs.listStatus(new Path(conf
				.get(Util.CONF_HEADER_PATH)).suffix("/"
				+ DataSetHeader.NUM_INSTANCES_DIRECTORY))) {
			if (!s.getPath().getName().startsWith("header")) {
				continue;
			}

			Util.log(DataSetHeader.class, "reading index file " + s.getPath());

			FSDataInputStream stream = fs.open(s.getPath());
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					stream));
			fileReaders.add(reader);
		}

		for (int i = 0; i < fileReaders.size(); i++) {
			BufferedReader r = fileReaders.get(i);

			do {
				line = r.readLine();

				if (line == null) {
					break;
				}

				rr = line.split("\t");
				if (rr.length != 2) {
					continue;
				}

				numInstances.put(rr[0], Integer.parseInt(rr[1]));
			} while (true);
		}

		return numInstances;
	}

	/**
	 * The name of the data set.
	 */
	private String name;

	/**
	 * The number of attributes.
	 */
	private int numAttributes;

	/**
	 * The number of classes.
	 */
	private int numClasses;

	/**
	 * Constructs a new instance of <i>DataSetHeader</i>.
	 * 
	 * @param conf
	 *            The hadoop <i>Configuration</i>
	 * @param headerPath
	 *            The path to the header files.
	 */
	public DataSetHeader(Configuration conf, Path headerPath) {
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);

			// open the meta file
			FSDataInputStream streamMetafile = fs.open(headerPath.suffix("/"
					+ DataSetHeader.META_DATA_FILE));
			BufferedReader metafileReader = new BufferedReader(
					new InputStreamReader(streamMetafile));

			// loop through the file
			do {
				String line = metafileReader.readLine();
				if (line == null) {
					break;
				}

				String[] rr = line.split("=");
				if (rr.length != 2) {
					continue;
				}

				// parse the meta file
				if (rr[0].equals("name")) {
					name = new String(rr[1]);
				}
				if (rr[0].equals("numAttributes")) {
					numAttributes = Integer.parseInt(rr[1]);
				}
				if (rr[0].equals("numClasses")) {
					numClasses = Integer.parseInt(rr[1]);
				}
			} while (true);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Returns the name of the data set.
	 * 
	 * @return The name of the data set.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the number of attributes.
	 * 
	 * @return Returns the number of attributes.
	 */
	public int getNumAttributes() {
		return numAttributes;
	}

	/**
	 * Returns the number of classes.
	 * 
	 * @return Returns the number of classes.
	 */
	public int getNumClasses() {
		return numClasses;
	}
}