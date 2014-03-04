package de.sec.dns.test;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.sec.dns.dataset.Instance;
import de.sec.dns.util.Util;

/**
 * The reducer class for {@link ConfusionMatrixTool}. It gathers all classified
 * instances and prints out additional information such as the true and false
 * positive rate.
 * 
 * @author Christian Banse, Elmo Randschau
 */
public class ConfusionMatrixReducer extends Reducer<Text, Text, Text, Text> {

	HashMap<String, Integer> numInstancesPerClass;
	int overallNumInstances; // number of all the instances of all the classes
	// in all the worlds

	ArrayList<String> unknownInstances = new ArrayList<String>();

	// get the number of classes
	int numClasses;

	@Override
	protected void reduce(Text classLabel, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		int numTruePositives = 0;
		int numFalsePositives = 0;
		int numTotalClassified = 0;

		String[] rr;
		String assignedInstance;
		StringBuilder builder = new StringBuilder();

		float cosineSimilarity;

		float highestSimilarity = 0.0f;
		String mostSimilarClass = null;

		ArrayList<String> assignedInstances = new ArrayList<String>();
		Configuration conf = context.getConfiguration();

		// loop through all values and fill the true/false positive variables
		for (Text val : values) {
			rr = val.toString().split("\t");
			if (rr.length != 2) {
				continue;
			}
			assignedInstance = rr[0];

			if ((conf.get(Util.CONF_CLASSIFIER)).equals("lift")
					|| (conf.get(Util.CONF_CLASSIFIER)).equals("support")) {
				// yang
				// Yang doesn't use cosineSimilarity .. so we set a constant
				// value for all users
				cosineSimilarity = 0.0f;
			}

			else {
				// mnb...
				cosineSimilarity = Float.parseFloat(rr[1]);
			}

			assignedInstances.add(assignedInstance);

			if (cosineSimilarity > highestSimilarity) {
				highestSimilarity = cosineSimilarity;
				mostSimilarClass = assignedInstance;
			}
		}

		// we have more than one instances assigned to this class
		// now we try to pick one class based on the cosine similarity unless
		// we have a similarity of 0.0
		if (Util.isEnabled(context.getConfiguration(),
				Util.CONF_TEST_DROP_AMBIGUOUS_RESULTS)) {
			if (assignedInstances.size() > 1) {
				unknownInstances.addAll(assignedInstances);

				assignedInstances.clear();

				// nur wenn zumindest eine der zugewiesenen Instanzen eine
				// Similarity von >0 hatte
				// wird diese der Klasse nun zugewiesen
				if (highestSimilarity > 0.0f) {
					assignedInstances.add(mostSimilarClass);
					unknownInstances.remove(mostSimilarClass);
				}
			}
		}

		for (String instance : assignedInstances) {
			rr = instance.toString().split(Instance.ID_SEPARATOR);
			String className = rr[0];

			if (className.equals(classLabel.toString())) {
				numTruePositives++;
			} else {
				numFalsePositives++;
			}
			numTotalClassified++;

			builder.append(instance + "\t");
		}

		int numInstances = 0;

		if (numInstancesPerClass.containsKey(classLabel.toString())) {
			numInstances = numInstancesPerClass.get(classLabel.toString());
		}

		double tpRate = 0; // True-Positive-Rate = Recall
		double fpRate = 0; // False-Positive-Rate
		double precision = 0; // Precision = True-Positives / (True-Positives +
		// False-Positives)

		// calculate the true positive rate
		if (numInstances > 0) {
			tpRate = (double) numTruePositives / numInstances;
		}

		// calculate the false positive rate
		if (numFalsePositives > 0) {
			fpRate = (double) numFalsePositives
					/ (overallNumInstances - numInstances);
		}

		// calculate the precision
		if (numTotalClassified > 0) {
			precision = (double) numTruePositives / numTotalClassified;
		}

		// write it to the matrix file
		Text t = new Text(numTotalClassified + "\t" + Util.format(tpRate)
				+ "\t" + Util.format(fpRate) + "\t" + Util.format(precision)
				+ "\t" + builder.toString());

		context.write(classLabel, t);
	}

	public void setup(Context context) {
		numClasses = context.getConfiguration()
				.getInt(Util.CONF_NUM_CLASSES, 1);

		try {
			readInstancesPerClass(context);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void readInstancesPerClass(Context context) throws IOException {
		String line = null;
		String rr[] = null;
		Path file = null;

		Configuration conf = context.getConfiguration();

		// TODO: might cause problems with openworld
		Path instancesPerClass = new Path(conf.get(Util.CONF_TRAINING_PATH)
				+ "/" + conf.get(Util.CONF_SESSION_DURATION) + "/"
				+ conf.get(Util.CONF_OPTIONS) + "/"
				+ Util.INSTANCES_PER_CLASS_PATH + "/"
				+ conf.get(Util.CONF_TEST_DATE));
		FileSystem fs = FileSystem.get(conf);

		numInstancesPerClass = new HashMap<String, Integer>();

		int value;
		overallNumInstances = 0;

		for (FileStatus fileStatus : fs.listStatus(instancesPerClass)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();

			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			while (true) {
				line = reader.readLine();
				if (line == null) {
					break;
				}
				rr = line.split("\t");

				// rr[0] class
				// rr[1] number of instances
				value = (int) Double.parseDouble(rr[1]);
				numInstancesPerClass.put(rr[0], value);
				overallNumInstances += value;
			}
		}
	}

	@Override
	public void cleanup(Context context) {
		try {
			if (unknownInstances.size() > 0) {
				StringBuilder builder = new StringBuilder();
				for (String instance : unknownInstances) {
					builder.append(instance + "\t");
				}

				double tpRate = 0;
				double fpRate = (double) unknownInstances.size() / (numClasses);

				// precision does not make sense for "unknown" class
				double precision = 0;

				Text t = new Text(unknownInstances.size() + "\t"
						+ Util.format(tpRate) + "\t" + Util.format(fpRate)
						+ "\t" + Util.format(precision) + "\t"
						+ builder.toString());

				context.write(new Text("UNKNOWN"), t);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
