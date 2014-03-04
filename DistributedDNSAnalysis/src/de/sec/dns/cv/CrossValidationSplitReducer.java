package de.sec.dns.cv;

import gnu.trove.map.hash.TIntIntHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.analysis.DNSAnalysisMapper;
import de.sec.dns.util.Util;

/**
 * The reducer class for {@link CrossValidationSplitTool}. It creates multiple
 * splits of the dataset for cross validation
 * 
 * @author Christian Banse
 */
public class CrossValidationSplitReducer extends
		Reducer<Text, Text, NullWritable, Text> {
	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<NullWritable, Text> m;

	/**
	 * A temporary variable for the mapreduce value.
	 */
	private DoubleWritable v = new DoubleWritable();

	// entspricht numFolds, also 10 bei 10-fold cv
	private int numSplits;

	private int seed;

	private int numInstancesPerClass;
	private int numTrainingInstancesPerClass;

	private Random random;

	private static Log LOG = LogFactory
			.getLog(CrossValidationSplitReducer.class);

	@Override
	public void cleanup(Context context) {
		try {
			// close the multiple output handler
			m.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * values are instances (in text representation)
	 */
	@Override
	protected void reduce(Text classLabel, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();

		// for each class we retrieve all instances
		ArrayList<String> instancesOfUser = new ArrayList<String>(
				numInstancesPerClass);

		for (Text instance : values) {
			instancesOfUser.add(instance.toString());
		}

		// bei 10-fold crossvalidation m�ssen wir nun 10 teile bilden und alle
		// varianten in den splits abbilden

		String[][] splitsWithInstances = new String[numSplits][numInstancesPerClass
				/ numSplits];
		for (int j = 0; j < numSplits; j++) {
			for (int i = 0; i < (numInstancesPerClass / numSplits); i++) {
				String instance = instancesOfUser.remove(random
						.nextInt(instancesOfUser.size()));
				splitsWithInstances[j][i] = instance;
			}
		}

		TIntIntHashMap numTrainingInstancesWrittenPerSplit = new TIntIntHashMap();

		for (int i = 0; i < numSplits; i++) {
			int instanceId = 1;
			for (int j = 0; j < numSplits; j++) {
				String outputPath;
				if (i == j) {
					// test
					outputPath = "2010-01-02-00-00";

				} else {
					// training
					outputPath = "2010-01-01-00-00";

				}
				for (int k = 0; k < splitsWithInstances[j].length; k++) {
					String instance = splitsWithInstances[j][k];

					// reduce number of training instances?
					if (numTrainingInstancesPerClass > 0) {
						if (i != j) { // training instance
							int numTrainingInstancesWrittenForSplit = numTrainingInstancesWrittenPerSplit
									.get(i);
							if (numTrainingInstancesWrittenForSplit >= numTrainingInstancesPerClass) {
								// LOG.info("skipping instance for"+classLabel);
								continue; // skip the instance if we have enough
								// training instances
							}

							numTrainingInstancesWrittenPerSplit
									.adjustOrPutValue(i, 1, 1);
							// write it (below)
						} else {

						}
					}
					// have to increment instance id
					StringBuilder newInstance = new StringBuilder();
					newInstance.append(instance.substring(0,
							instance.lastIndexOf(',')));
					newInstance.append(',').append(instanceId++);
					// LOG.info("writing "+newInstance);

					m.write(NullWritable.get(),
							new Text(newInstance.toString()), "split" + i + "/"
									+ conf.get(Util.CONF_SESSION_DURATION)
									+ "/" + conf.get(Util.CONF_OPTIONS) + "/"
									+ outputPath + "/");
				}
			}

		}

		/*
		 * Neue Idee: statt einem Array shuffledInstancesOfUser verwenden wir
		 * folds viele Arrays in denen ein foldster Teil der Instances liegt.
		 * Diese werden zuf�llig bef�llt. Die Folds werden dann so
		 * generiert, dass man in jedem Fold den Split mit der aktuellen
		 * Fold-Nummer zum Testen verwendet und alle anderen Splits sind
		 * Trainingsdaten.
		 */

	}

	@Override
	public void setup(Context context) {
		try {
			Configuration conf = context.getConfiguration();

			// initialize the multiple outputs handler
			m = new MultipleOutputs<NullWritable, Text>(context);

			numSplits = conf.getInt(Util.CONF_CROSS_VALIDATION_NUM_FOLDS, 0);
			seed = conf.getInt(Util.CONF_CROSS_VALIDATION_SEED, 0);
			numInstancesPerClass = conf.getInt(
					Util.CONF_CROSS_VALIDATION_NUM_INSTANCES_PER_CLASS, 0);
			random = new Random(seed);

			numTrainingInstancesPerClass = conf
					.getInt(Util.CONF_CROSS_VALIDATION_NUM_TRAINING_INSTANCES_PER_CLASS,
							0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
