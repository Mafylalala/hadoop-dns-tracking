package de.sec.dns.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * An implementation of a confusion matrix. It holds information about which
 * instance has been classified as which class.
 * 
 * @author Christian Banse
 */
public class ConfusionMatrix extends HashMap<String, ConfusionMatrixEntry> {
	/**
	 * The serial version uid.
	 */
	private static final long serialVersionUID = 3921764228138016801L;

	/**
	 * Reads a confusion matrix from the hdfs.
	 * 
	 * @param fs
	 *            A valid {@link FileSystem} object.
	 * @param string
	 *            The path to the hdfs files.
	 * @throws IOException
	 *             if an error occurs.
	 */
	public ConfusionMatrix(FileSystem fs, String string) throws IOException {
		FSDataInputStream fis = fs.open(new Path(string));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		String line = null;

		// loop through the file
		while (true) {
			// read one line
			line = reader.readLine();

			if (line == null) {
				break;
			}

			ConfusionMatrixEntry entry = new ConfusionMatrixEntry(line);

			// put the entry into the confusion matrix
			put(entry.getClassLabel(), entry);
		}

		fis.close();
		reader.close();
	}

	/**
	 * Retrieves an entry from the confusion matrix.
	 * 
	 * @param classLabel
	 *            The desired class.
	 * @return The entry from the confusion matrix if the class is present, an
	 *         empty entry otherwise.
	 */
	public ConfusionMatrixEntry get(String classLabel) {
		ConfusionMatrixEntry entry = super.get(classLabel);

		if (entry == null) {
			entry = new ConfusionMatrixEntry();
		}

		return entry;
	}
}
