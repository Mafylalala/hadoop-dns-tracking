package de.sec.dns.training;

import gnu.trove.map.hash.TIntDoubleHashMap;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * The reducer class for {@link TrainingTool}. It sums all the occurence tuples
 * and calculates the bayesian probability.
 * 
 * @author Christian Banse
 */
public class TrainingReducer extends
		Reducer<Text, DoubleArrayWritable, Text, DoubleWritable> {
	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(TrainingReducer.class);

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<Text, DoubleWritable> m;

	private HashMap<String, Integer> numInstances = new HashMap<String, Integer>();

	/**
	 * A temporary variable for the mapreduce value.
	 */
	private DoubleWritable v = new DoubleWritable();

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
	protected void reduce(Text dateAndClassLabel,
			Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		TIntDoubleHashMap map = new TIntDoubleHashMap();
		// HashMap<Integer, Double> map = new HashMap<Integer, Double>();
		double wordsPerClass = 0;

		String[] str = dateAndClassLabel.toString().split(" ");
		String prob = str[0];

		if (prob.equals(Util.PROBABILITY_OF_WORD_GIVEN_CLASS_PATH)) {
			String date = str[1];

			// openworld simulation

			String classLabel = str[2];

			// if ((counter++ % 10) == 0) {
			LOG.info("reducing instance " + classLabel);
			// }

			// loop through all values, put them into an
			// hashmap and calculate the wordsPerClass
			int valueCounter = 0;
			for (DoubleArrayWritable rr : values) {
				Writable[] w = rr.get();
				// w[0] = attributeIndex
				// w[1] = numOccurences
				double occ = ((DoubleWritable) w[1]).get();
				wordsPerClass += occ;

				int attributeIndex = (int) ((DoubleWritable) w[0]).get();

				double value = map.get(attributeIndex);
				map.put(attributeIndex, occ + value);

				Util.ping(context, TrainingReducer.class);

				valueCounter++;
				if (valueCounter % 10000 == 0)
					Util.getMemoryInfo(TrainingReducer.class);
			}

			int numAttributes = context.getConfiguration().getInt(
					Util.CONF_NUM_ATTRIBUTES, 0);

			map.put(Util.MISSING_ATTRIBUTE_INDEX, 0.0);

			// calculate the probabilities for all members of the hashmap

			for (int attributeIndex : map.keys()) {
				double occ = map.get(attributeIndex);

				Util.ping(context, TrainingReducer.class);

				Text k = new Text();
				k.set(classLabel + "\t" + attributeIndex);
				v.set(Util.bayes(occ, wordsPerClass, numAttributes));
				m.write(k, v, Util.PROBABILITY_OF_WORD_GIVEN_CLASS_PATH + "/"
						+ date + "/");
			}
		} else if (prob.equals(Util.PROBABILITY_OF_CLASS_PATH)) {
			String date = str[1];
			String classLabel = str[2];

			int numInstancesAtDate = numInstances.get(date);

			int numClasses = context.getConfiguration().getInt(
					Util.CONF_NUM_CLASSES, 0);

			// loop through all values
			double instancesPerClass = 0;

			for (DoubleArrayWritable rr : values) {
				Writable[] w = rr.get();

				instancesPerClass += ((DoubleWritable) w[0]).get();

				Util.ping(context, TrainingReducer.class);
			}

			Text k = new Text();
			k.set(classLabel);
			v.set(Util.bayesPrH(instancesPerClass, numInstancesAtDate,
					numClasses));
			m.write(k, v, Util.PROBABILITY_OF_CLASS_PATH + "/" + date + "/");

			k.set(classLabel);
			v.set(instancesPerClass);
			m.write(k, v, Util.INSTANCES_PER_CLASS_PATH + "/" + date + "/");
		}
	}

	@Override
	public void setup(Context context) {
		try {
			// initialize the multiple outputs handler
			m = new MultipleOutputs<Text, DoubleWritable>(context);

			// read number of instances from dataset header
			numInstances = DataSetHeader.getNumInstances(context
					.getConfiguration());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
