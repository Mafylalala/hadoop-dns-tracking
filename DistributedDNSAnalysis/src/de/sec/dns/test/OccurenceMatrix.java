package de.sec.dns.test;

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
import org.apache.hadoop.mapreduce.Mapper.Context;

import de.sec.dns.analysis.DNSAnalysisMapper;
import de.sec.dns.util.Util;

/**
 * The occurrence matrix. Basically just a hashmap for convince issues and to
 * satisfy the java compiler.
 * 
 * @author Christian Banse
 */
public class OccurenceMatrix extends HashMap<String, Boolean> {

	/**
	 * A logger.
	 */
	private static Log LOG = LogFactory.getLog(OccurenceMatrix.class);

	/**
	 * The serial version uid.
	 */
	private static final long serialVersionUID = -5313153798023921744L;

	/**
	 * Retrieves an <i>OccurenceMatrix<i> for a given training session.
	 * 
	 * @param conf
	 *            The current <i>Configuration</i>.
	 * @param session
	 *            The training session.
	 * @return The <i>OccurenceMatrix</i> for the given training session.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static OccurenceMatrix getOccurenceData(Context context,
			Configuration conf, String session) throws IOException {

		String line = null;
		String rrr = null;
		String rr[] = null;

		OccurenceMatrix occ = new OccurenceMatrix();
		Path file = null;
		Path trainingPath = null;

		if ((conf.get(Util.CONF_CLASSIFIER)).equals("lift")
				|| (conf.get(Util.CONF_CLASSIFIER)).equals("support")) {
			trainingPath = new Path(conf.get(Util.CONF_TRAINING_PATH) + "/"
					+ conf.get(Util.CONF_SESSION_DURATION) + "/"
					+ conf.get(Util.CONF_OPTIONS) + "/"
					+ conf.get("classifier") + "/" + session);
		}

		else if ((conf.get(Util.CONF_CLASSIFIER)).equals("mnb")
				|| (conf.get(Util.CONF_CLASSIFIER)).equals("jaccard")
				|| (conf.get(Util.CONF_CLASSIFIER)).equals("cosim")) {
			trainingPath = new Path(conf.get(Util.CONF_TRAINING_PATH) + "/"
					+ conf.get(Util.CONF_SESSION_DURATION) + "/"
					+ conf.get(Util.CONF_OPTIONS) + "/"
					+ Util.PROBABILITY_OF_WORD_GIVEN_CLASS_PATH + "/" + session);
		}

		FileSystem fs = FileSystem.get(conf);

		LOG.info("Reading occurence data from " + trainingPath);

		// loop through all files in the trainingPath
		for (FileStatus fileStatus : fs.listStatus(trainingPath)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();
			LOG.info("Reading occurence data from file " + file);

			// open the file
			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));
			int i = 0;

			// loop through the file
			while (true) {
				line = reader.readLine();

				if (i++ % 50000 == 0) {
					if (context != null) {
						Util.ping(context, DNSAnalysisMapper.class);
					}
					i = 0;
				}

				if (line == null) {
					break;
				}
				if ((conf.get(Util.CONF_CLASSIFIER)).equals("lift")
						|| (conf.get(Util.CONF_CLASSIFIER)).equals("support")) {
					// braucht weniger RAM
					// damit der lange String "line" nicht aufbewahrt wird
					StringBuilder sb = new StringBuilder();
					sb.append(line.substring(0, line.indexOf("\t")));
					rrr = sb.toString();
					line = null;
					// we only need the class name, put it into the
					// OccurenceMatrix
					occ.put(rrr, true);
				}

				else if ((conf.get(Util.CONF_CLASSIFIER)).equals("mnb")
						|| (conf.get(Util.CONF_CLASSIFIER)).equals("jaccard")
						|| (conf.get(Util.CONF_CLASSIFIER)).equals("cosim")) {
					// rr[0] class
					// rr[1] attributeIndex
					// rr[2] value
					rr = line.split("\t");
					// we only need the class name, put it into the
					// OccurenceMatrix
					occ.put(rr[0], true);
				}
			}
			reader.close();
			in.close();
			LOG.info("Read " + i + " lines. Occurence map size: " + occ.size());
			Util.getMemoryInfo(DNSAnalysisMapper.class);
		}
		line = null;
		return occ;
	}

}
