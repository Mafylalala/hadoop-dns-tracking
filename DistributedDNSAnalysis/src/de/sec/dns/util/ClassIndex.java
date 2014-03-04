package de.sec.dns.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import de.sec.dns.dataset.DataSetHeader;

/**
 * The class index. This class provides access to a list of all classes and
 * their numeric index. This class is a singleton.
 * 
 * @author Christian Banse, Dominik Herrmann
 */
public class ClassIndex {
	/**
	 * The singleton.
	 */
	private static ClassIndex _classIndex;

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(ClassIndex.class);

	/**
	 * Retrieves or creates the instance of this class.
	 * 
	 * @return The instance of <i>ClassIndex</i>.
	 */
	public static ClassIndex getInstance() {
		if (_classIndex == null) {
			_classIndex = new ClassIndex();
		}

		return _classIndex;
	}

	/**
	 * This array holds all class names.
	 */
	private String[] classNames = null;

	/**
	 * A cached <i>Configuration</i>.
	 */
	private Configuration conf;

	/**
	 * Empties the index, freeing memory.
	 */
	public void free() {
		conf = null;
		classNames = null;
	}

	/**
	 * Returns the class name of the specified index.
	 * 
	 * @param i
	 *            The desired index.
	 * @return The class name of the index.
	 */
	public String getClassname(int i) {
		return classNames[i];
	}

	/**
	 * Returns index position of the class name in the index or a value < 0 if
	 * the given class name is not contained in the index. It uses a binary
	 * search.
	 * 
	 * @param classname
	 *            The class name.
	 * @return The index of the class name.
	 */
	public int getIndexPosition(String classname) {
		return java.util.Arrays.binarySearch(classNames, classname);
	}

	/**
	 * Returns the size of the index.
	 * 
	 * @return The size of the index.
	 */
	public int getSize() {
		return classNames.length;
	}

	/**
	 * Initializes the class index with the given configuration.
	 * 
	 * @param conf
	 *            A <i>Configuration</i>.
	 * @throws Exception
	 */
	public void init(Configuration conf) throws Exception {
		if (this.conf == null) {
			this.conf = conf;
		} else {
			throw new Exception(
					"init has already been called; cannot initialize this index again");
		}
	}

	/**
	 * Returns if the index is already populated.
	 * 
	 * @return True if the index is populated, false otherwise.
	 */
	public boolean isPopulated() {
		return (classNames != null);
	}

	/**
	 * Populates the index.
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void populateIndex() throws Exception {
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path(conf.get(Util.CONF_HEADER_PATH));

		LOG.info("starting to populate the index...");

		Util.getMemoryInfo(ClassIndex.class);

		String line = null;

		ArrayList<BufferedReader> fileReaders = new ArrayList<BufferedReader>();

		DataSetHeader header = new DataSetHeader(conf, path);

		// Array anlegen, jetzt wissen wir ja die Gr��e
		int arraySize = header.getNumClasses();
		if (arraySize <= 0) {
			throw new Exception(
					"Could not read numClasses from metadata or invalid value: "
							+ arraySize + 1);
		}

		LOG.info("will read " + arraySize + " classes.");

		classNames = new String[arraySize];

		for (FileStatus s : fs.listStatus(path.suffix("/"
				+ DataSetHeader.USER_INDEX_DIRECTORY))) {
			if (!s.getPath().getName().startsWith("header")) {
				continue;
			}

			LOG.info("reading index file " + s.getPath());
			FSDataInputStream stream = fs.open(s.getPath());
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					stream));
			fileReaders.add(reader);
		}

		ArrayList<String> readLines = new ArrayList<String>();

		// Aus allen Dateien die erste Zeile lesen
		for (int i = 0; i < fileReaders.size(); i++) {
			BufferedReader r = fileReaders.get(i);

			line = r.readLine();
			readLines.add(line);
		}

		int classIndexCounter = 0;

		while (true) {
			// Die "kleinste" Zeile finden, damit das hostNames Array auf jeden
			// Fall alphabetisch sortiert ist
			// das ist n�tig, damit wir sp�ter effizient mittels
			// binarySearch im
			// Index suchen k�nnen
			// dazu alle nicht-null-Zeilen in eine temp. Variable packen und
			// diese sortieren
			ArrayList<String> sortedLinesTemp = new ArrayList<String>();
			for (String l : readLines) {
				if (l != null) {
					sortedLinesTemp.add(l);
				}
			}

			// wenn keine nicht-null Elemente mehr �brig sind, sind alle
			// Reader
			// fertig
			if (sortedLinesTemp.size() == 0) {
				break;
			}

			String[] sortedLines = new String[sortedLinesTemp.size()];
			sortedLines = sortedLinesTemp.toArray(sortedLines);
			java.util.Arrays.sort(sortedLines);

			// das kleinste Element ist nun in sortedLines[0]; nun den Index des
			// Elements
			// in readLines ermitteln, um den betroffenen Reader zu ermitteln

			int minIndex = readLines.indexOf(sortedLines[0]);

			line = readLines.get(minIndex);

			String[] rr = line.split("\t");
			if (rr.length != 2) {
				continue;
			}

			classNames[classIndexCounter] = rr[0];
			classIndexCounter++;

			// vom betroffenen FileReader die n�chste Zeile einlesen
			readLines.set(minIndex, fileReaders.get(minIndex).readLine());

			if ((classIndexCounter % 10000) == 0) {
				LOG.info("number of elements in index " + classIndexCounter);
				Util.getMemoryInfo(ClassIndex.class);
			}
		}

		for (BufferedReader r : fileReaders) {
			r.close();
		}
		LOG.info("populateIndex is done");
	}

	public Set<String> toSet() {
		Set<String> set = new HashSet<String>();
		for (String clazz : classNames) {
			set.add(clazz);
		}

		return set;
	}

	public List<String> asArrayList() {
		return new ArrayList<String>(Arrays.asList(classNames));
	}
}
