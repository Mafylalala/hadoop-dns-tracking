package de.sec.dns.dataset;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import de.sec.dns.dataset.Instance;
import de.sec.dns.util.Util;

/**
 * The training mapper for {@link TrainingTool}. It hands an tuple consisting of
 * the attribute index and the occurrence of the attribute to the reducer.
 * 
 * @author Christian Banse
 */
public class CountEffectiveNumOfAttributesMapper extends
		Mapper<LongWritable, ObjectWritable, Text, DoubleWritable> {

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory
			.getLog(CountEffectiveNumOfAttributesMapper.class);

	/**
	 * A temporary variable used as mapreduce key.
	 */
	// contains class label
	private Text k = new Text();

	// contains attribute index (hostname
	private DoubleWritable v = new DoubleWritable();

	/**
	 * is set by setup method in case of fatal failure
	 */
	private Exception setupFailedException;

	private boolean TRAIN_WITH_LIMITED_DATASET;

	@Override
	public void map(LongWritable key, ObjectWritable obj, Context context)
			throws IOException, InterruptedException {
		Instance instance;

		if (setupFailedException != null) {
			throw new IOException(setupFailedException);
		}

		if (TRAIN_WITH_LIMITED_DATASET) {
			throw new IOException(
					"TRAIN WITH LIMITED DATASET IS NOT IMPLEMENTED");
		}
		FileSplit file = (FileSplit) context.getInputSplit();

		// Extract date from path
		String date = file.getPath().getParent().getName();

		instance = (Instance) obj.get();

		String userAtDate = instance.getClassLabel() + "@" + date;

		double numQueries = 0;
		// iterate over all hostnames
		for (int i = 0; i < instance.getNumIndices(); i++) {
			k.set(userAtDate);
			v.set(instance.getIndex(i));
			context.write(k, v);
			numQueries += instance.getValue(i);
		}
		k.set("NUMQUERIES_"+userAtDate);
		v.set(numQueries);
		context.write(k, v);
	}

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();

		TRAIN_WITH_LIMITED_DATASET = conf.getBoolean(
				Util.CONF_TRAINING_WITH_LIMITED_DATASET, false);

		float threshold = conf.getFloat(Util.CONF_MINIMUM_DF_OF_HOSTS, 0);

		if (threshold > 0.0) {
			setupFailedException = new Exception(
					"MINIMUM DF OF HOSTS IS NOT SUPPORTED");
			return;
		}
	}
}