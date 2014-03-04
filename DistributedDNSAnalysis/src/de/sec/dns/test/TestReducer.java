package de.sec.dns.test;

import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.Instance;
import de.sec.dns.util.ClassIndex;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * The reducer class for {@link TestTool}. It exponentiates all the logarithmic
 * probabilities and tries to find the greatest probability.
 * 
 * @author Christian Banse
 */
public class TestReducer extends Reducer<Text, DoubleArrayWritable, Text, Text> {

	/**
	 * Last access time. Used for performance evaluation.
	 */
	// private static long lastTime;

	/**
	 * just a counter
	 */
	private static int counter = 0;

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(TestReducer.class);

	/**
	 * The class index.
	 */
	private ClassIndex classIndex = ClassIndex.getInstance();

	/**
	 * Used to handle multiple output files.
	 */
	// private MultipleOutputs<Text, Text> m;

	/**
	 * The current <i>Context</i>
	 */
	private Context context;

	private HashMap<String, Integer> numInstances = new HashMap<String, Integer>();

	/**
	 * The probabilities of a word given class for the specified day.
	 */
	private TObjectFloatHashMap<String> probClass = new TObjectFloatHashMap<String>();

	public void readProbabiltyOfClass() throws IOException,
			ClassNotFoundException {
		String line = null;
		String rr[] = null;
		Path file = null;

		Configuration conf = context.getConfiguration();

		Path trainingPath = new Path(conf.get(Util.CONF_TRAINING_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/"
				+ conf.get(Util.CONF_OPTIONS) + "/"
				+ Util.PROBABILITY_OF_CLASS_PATH + "/"
				+ conf.get(Util.CONF_TRAINING_DATE));
		FileSystem fs = FileSystem.get(conf);

		for (FileStatus fileStatus : fs.listStatus(trainingPath)) {
			if (fileStatus.isDir()) {
				continue;
			}

			file = fileStatus.getPath();

			LOG.info("reading from " + file + "...");

			FSDataInputStream in = fs.open(file);
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			int i = 0;
			while (true) {
				line = reader.readLine();

				if (i++ % 10000 == 0) {
					Util.ping(context, TestMapper.class);
					i = 0;
				}

				if (line == null) {
					break;
				}

				rr = line.split("\t");

				// rr[0] class
				// rr[1] value

				String classLabel = rr[0];
				float value = Float.parseFloat(rr[1]);

				if (!probClass.contains(classLabel)) {
					probClass.put(classLabel, value);
				} else {
					reader.close();
					throw new IOException(
							"Duplicate classLabel in class probability file!");
				}
			}
		}

		line = null;
		rr = null;
	}

	@Override
	protected void reduce(Text instanceId,
			Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {

		// for now we only have instances on the same day, but maybe we'll
		// change that someday
		String date = context.getConfiguration().get(Util.CONF_TEST_DATE);

		TIntFloatHashMap map = new TIntFloatHashMap();
		TIntFloatHashMap cosimMap = new TIntFloatHashMap();

		if ((counter++ % 10) == 0) {
			LOG.info("reducing... " + instanceId);
		}

		// lastTime = System.currentTimeMillis();

		double max = 0.0;
		int i = 0;

		// find the greatest value
		for (DoubleArrayWritable d : values) {
			DoubleWritable cosim = (DoubleWritable) d.get()[2];
			DoubleWritable classIndex = (DoubleWritable) d.get()[1];
			DoubleWritable value = (DoubleWritable) d.get()[0];

			cosimMap.put((int) classIndex.get(), (float) cosim.get());

			if ((value.get() > max) || (i == 0)) {
				max = value.get();
			}
			map.put((int) classIndex.get(), (float) value.get());
			i++;
		}

		// long timeTaken = System.currentTimeMillis() - lastTime;

		// LOG.info("writing values into hashmap... " + timeTaken + " ms");

		int numClasses = context.getConfiguration().getInt(
				Util.CONF_NUM_CLASSES, 0);

		String trainingDate = context.getConfiguration().get(
				Util.CONF_TRAINING_DATE);

		// LOG.info("testing " + instanceId + " against...");

		int numInstancesAtTrainingDate = numInstances.get(trainingDate);

		for (TIntFloatIterator it = map.iterator(); it.hasNext();) {
			// exponate and normalize the values
			it.advance();

			// OLD:
			// it.setValue((float)Math.exp(it.value() - max));

			// NEW:
			double prc = probClass.get(classIndex.getClassname(it.key()));

			// class was not present at the training session
			if (prc == 0) {
				prc = Util.bayesPrH(0, numInstancesAtTrainingDate, numClasses);
			}

			if (context.getConfiguration().getBoolean(Util.CONF_USE_PRC, false)) {
				float result = (float) (Math.exp(it.value() - max) * prc);
				// LOG.info("... " + classIndex.getClassname(it.key()) +
				// "\tvalue:\t" + it.value() + "\tprc:\t" + prc + "\tmax:\t" +
				// max + "\tresult:\t" + result);
				it.setValue(result);
			} else {
				float result = (float) (Math.exp(it.value() - max));
				// LOG.info("... " + classIndex.getClassname(it.key()) +
				// "\tvalue:\t" + it.value() + "\tmax:\t" + max + "\tresult:\t"
				// + result);
				it.setValue(result);
			}
		}

		// long timeTaken = System.currentTimeMillis() - lastTime;
		// lastTime = System.currentTimeMillis();

		// LOG.info("normalizing and exponentiating... " + timeTaken + " ms");

		max = 0;
		int maxIndex = 0;

		// find the greatest value again and store its index

		// for some reason, Trove map keys are stored in reversed order...
		// so to simulate weka behaviour we traverse this map in reverse order

		// OLD CODE
		/*
		 * for (TIntFloatIterator it = map.iterator(); it.hasNext();) {
		 * it.advance(); float d = it.value();
		 * 
		 * if (d > max) { max = d; maxIndex = it.key(); } }
		 */

		int key;
		double value;

		for (int j = map.size() - 1; j >= 0; j--) {
			key = map.keys()[j];
			value = map.get(key);

			if (value > max) {
				max = value;
				maxIndex = key;
			}
		}

		// LOG.info("classified as: "+ classIndex.getClassname(maxIndex));
		// timeTaken = System.currentTimeMillis() - lastTime;

		// LOG.info("finding greatest value... " + timeTaken + " ms");
		// lastTime = System.currentTimeMillis();

		// increase the counters
		try {
			// split class name from instance id
			String[] rr = instanceId.toString().split(Instance.ID_SEPARATOR);
			String className = rr[0];

			if (className.equals(classIndex.getClassname(maxIndex))) {
				context.getCounter(TestTool.TestCounter.CORRECTLY_CLASSIFIED)
						.increment(1);
			} else {
				context.getCounter(
						TestTool.TestCounter.NOT_CORRECTLY_CLASSIFIED)
						.increment(1);
			}
			context.write(instanceId,
					new Text(classIndex.getClassname(maxIndex) + "\t"
							+ cosimMap.get(maxIndex))/*
													 * ,
													 * Util.PREDICTED_CLASSES_PATH
													 * + "/" + date + "/"
													 */);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@Override
	public void cleanup(Context context) {
		/*
		 * try { // close the multiple output handler m.close(); } catch
		 * (Exception e) { e.printStackTrace(); }
		 */
	}

	@Override
	public void setup(Context context) {
		this.context = context;

		try {
			// initialize the multiple outputs handler
			// m = new MultipleOutputs<Text, Text>(context);

			if (!classIndex.isPopulated()) {
				classIndex.init(context.getConfiguration());
				classIndex.populateIndex();
			}
			LOG.info("reading class probabilities...");

			readProbabiltyOfClass();

			numInstances = DataSetHeader.getNumInstances(context
					.getConfiguration());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
