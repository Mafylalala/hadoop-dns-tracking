package de.sec.dns.test;

import gnu.trove.map.hash.TLongFloatHashMap;

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
import de.sec.dns.util.ClassIndex;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * Training mapper for {@link YangTestTool}. It hands an tuple consisting of the
 * attribute index and the occurrence of the attribute to the reducer.
 * 
 * @author Elmo Randschau
 * 
 */
public class YangTestMapper extends
		Mapper<LongWritable, ObjectWritable, Text, DoubleArrayWritable> {

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(YangTestMapper.class);

	/**
	 * The class index.
	 */
	private ClassIndex classIndex;

	/**
	 * The current <i>Context</i>
	 */
	private Context context;

	/**
	 * This hashmap contains all hosts that meet the minimum DF criterion. Only
	 * hosts included in this map are included.
	 * 
	 * If this map is null, all hosts will be included regardless of their DF.
	 */
	private TLongFloatHashMap hostsWithMinimumDF = null;

	/**
	 * is set by setup method in case of fatal failure
	 */
	private Exception setupFailedException;

	public static char NO_INSTANCE = '-';

	/**
	 * A temporary variable used as mapreduce key.
	 */
	private Text k = new Text();
	/**
	 * A temporary variable for the mapreduce value.
	 */
	private DoubleArrayWritable v = new DoubleArrayWritable();

	@Override
	public void map(LongWritable key, ObjectWritable obj, Context context)
			throws IOException, InterruptedException {

		if (setupFailedException != null) {
			throw new IOException(setupFailedException);
		}
		FileSplit file = (FileSplit) context.getInputSplit();

		// Extract date from path
		String testdate = file.getPath().getParent().getName();
		String classLabel;
		Instance instance = (Instance) obj.get();

		// remove all hosts whose DF is below the threshold
		if (hostsWithMinimumDF != null) {
			instance.setAcceptableIndices(hostsWithMinimumDF.keySet());
		}
		classLabel = instance.getClassLabel();

		writeInstance(context, instance, classLabel, testdate);

	}

	private void writeInstance(Context context, Instance instance,
			String classLabel, String date) throws IOException,
			InterruptedException {
		double numOccurences;
		for (int a = 0; a < instance.getNumValues(); a++) {
			numOccurences = instance.getValue(a);
			if (numOccurences < 0) {
				throw new IOException(
						"Numeric attribute values must all be greater or equal to zero.");
			}

			// key = profiling date classLabel
			// value = {hostIndex,numOccurence}

			long indexOfHost = instance.getIndex(a);
			if ((hostsWithMinimumDF == null)
					|| ((hostsWithMinimumDF != null) && hostsWithMinimumDF
							.contains(indexOfHost))) {

				k.set(classLabel + "\t" + date);

				v.set(new DoubleWritable[] { new DoubleWritable(indexOfHost),
						new DoubleWritable(numOccurences) });

				context.write(k, v);
			}
		}
	}

	@Override
	public void setup(Context context) {

		this.context = context;

		classIndex = ClassIndex.getInstance();

		Configuration conf = context.getConfiguration();

		float threshold = conf.getFloat(Util.CONF_MINIMUM_DF_OF_HOSTS, 0);

		try {
			LOG.info("loading training data...");
			// readOverallDocumentFrequencies(context);

			if (threshold > 0.0) {
				LOG.info("loading DF values");
				hostsWithMinimumDF = Util.getHostsWithDocumentFrequencies(conf,
						threshold);
			}

			if (!classIndex.isPopulated()) {
				classIndex.init(conf);
				classIndex.populateIndex();
			}
		} catch (Exception e) {
			LOG.error("setup failed with an exception!");
			e.printStackTrace();
			setupFailedException = e;
		}
	}

}
