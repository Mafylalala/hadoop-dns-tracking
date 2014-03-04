package de.sec.dns.test;

import gnu.trove.map.hash.TIntFloatHashMap;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.sec.dns.dataset.Instance;
import de.sec.dns.util.ClassIndex;
import de.sec.dns.util.DoubleArrayWritable;

/**
 * The reducer class for {@link TestTool}. It exponentiates all the logarithmic
 * probabilities and tries to find the greatest probability.
 * 
 * @author Christian Banse
 */
public class JaccardTestReducer extends
		Reducer<Text, DoubleArrayWritable, Text, Text> {

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
	private static final Log LOG = LogFactory.getLog(JaccardTestReducer.class);

	/**
	 * The class index.
	 */
	private ClassIndex classIndex = ClassIndex.getInstance();

	@Override
	protected void reduce(Text instanceId,
			Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {

		if ((counter++ % 10) == 0) {
			LOG.info("reducing... " + instanceId);
		}

		double max = 0.0;
		int i = 0;
		int maxIndex = 0;

		// find the greatest value
		for (DoubleArrayWritable d : values) {
			DoubleWritable classIndex = (DoubleWritable) d.get()[0];
			DoubleWritable value = (DoubleWritable) d.get()[1];

			if ((value.get() > max) || (i == 0)) {
				max = value.get();
				maxIndex = (int) classIndex.get();

			}
			i++;
		}

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
					new Text(classIndex.getClassname(maxIndex) + "\t" + max));
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@Override
	public void cleanup(Context context) {

	}

	@Override
	public void setup(Context context) {
		try {

			if (!classIndex.isPopulated()) {
				classIndex.init(context.getConfiguration());
				classIndex.populateIndex();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
