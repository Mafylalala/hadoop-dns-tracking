package de.sec.dns.test;

import gnu.trove.impl.hash.TObjectHash;
import gnu.trove.list.linked.TLinkedList;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.THashSet;
import gnu.trove.set.hash.TLinkedHashSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.Instance;
import de.sec.dns.util.ClassIndex;
import de.sec.dns.util.DoubleArrayWritable;
import de.sec.dns.util.Util;

/**
 * The mapper class for {@link TestTool}. It calculates the probabilty of each
 * class for a given instance.
 * 
 * @author Christian Banse
 */
public class CosimTestMapper extends
		Mapper<LongWritable, ObjectWritable, Text, DoubleArrayWritable> {

	/**
	 * Counts the mapping entries. Used for the performance evaluation.
	 */
	private static int counter = 0;

	/**
	 * Holds the last time the map function was called. Used for the performance
	 * evaluation.
	 */
	private static long lastTime;

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(CosimTestMapper.class);

	/**
	 * Holds the total time the job has been mapping. Used for the performance
	 * evaluation.
	 */
	private static long totalTime = 0;

	/**
	 * A temporary variable for the value of the mapreduce entry.
	 */
	private DoubleArrayWritable array = new DoubleArrayWritable();

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

	/**
	 * if at least this time is needed for mapping a single instance it will be
	 * reported
	 */
	private long REPORT_SLOW_INSTANCE_THRESHOLD = 60;

	private DataSetHeader header;

	private TLinkedHashSet<Instance> trainingInstances;

	public static char NO_INSTANCE = '-';

	private float cosim(Instance instA, Instance instB) {
		TIntDoubleHashMap indicesValuesA = new TIntDoubleHashMap(
				instA.getNumIndices());

		for (int i = 0; i < instA.getNumIndices(); i++) {
			indicesValuesA.put(instA.getIndex(i), instA.getValue(i));
		}

		double dotProduct = 0;
		for (int i = 0; i < instB.getNumIndices(); i++) {
			int hostIndex = instB.getIndex(i);
			if (indicesValuesA.contains(hostIndex)) {
				double valueA = indicesValuesA.get(hostIndex);
				double valueB = instB.getValue(i);
				dotProduct += valueA * valueB;
			}
		}

		return (float) (dotProduct / (norm(instA) * norm(instB)));
	}

	private double norm(Instance inst) {
		double sum = 0;
		for (int i = 0; i < inst.getNumValues(); i++) {
			double val = inst.getValue(i);
			sum += val * val;
		}
		return Math.sqrt(sum);
	}

	@Override
	public void map(LongWritable key, ObjectWritable obj, Context context)
			throws IOException, InterruptedException {

		if (setupFailedException != null) {
			throw new IOException(setupFailedException);
		}

		DoubleWritable classI = new DoubleWritable();
		DoubleWritable value = new DoubleWritable();

		lastTime = System.currentTimeMillis();

		Instance instance;

		instance = (Instance) obj.get();

		// remove all hosts whose DF is below the threshold
		if (hostsWithMinimumDF != null) {
			instance.setAcceptableIndices(hostsWithMinimumDF.keySet());
		}

		// loop through training instances
		for (Instance trainingInstance : trainingInstances) {
			try {
				// class of test instance was present on the training day
				float cosimValue = cosim(trainingInstance, instance);

				int trainingClassId = classIndex
						.getIndexPosition(trainingInstance.getClassLabel());

				classI.set(trainingClassId);
				value.set(cosimValue);
				// store it in an array with the classIndex
				array.set(new DoubleWritable[] { classI, value });

				// and hand it to the reducer
				context.write(new Text(instance.getId()), array);
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("map failed with exception");
				throw new IOException(e);
			}
		}

		// count the number of instances per class
		// context.write(new Text(Util.INSTANCES_PER_CLASS_PATH + " " +
		// instance.getClassLabel()), ONE);

		counter++;

		long timeTaken = System.currentTimeMillis() - lastTime;
		totalTime += timeTaken;

		if ((counter % 10) == 0) {
			// print out some performance stuff
			LOG.info("instance " + counter + " duration: "
					+ ((double) timeTaken / 1000) + " s - avg : "
					+ ((double) (totalTime / counter) / 1000) + " s"
					+ " num_values: " + instance.getNumValues());
		}

		double duration = ((double) timeTaken / 1000);
		if (duration > REPORT_SLOW_INSTANCE_THRESHOLD) {
			LOG.info("Mapped a particularly SLOW INSTANCE. classLabel: "
					+ instance.getClassLabel() + ", " + "duration: " + duration
					+ " s (" + duration / 60 + " min)," + " num_values: "
					+ instance.getNumValues());
		}

	}

	public void loadTrainingInstances(String date) throws IOException,
			ClassNotFoundException {
		String line = null;

		// TIntByteHashMap map = null;
		Path file = null;

		Configuration conf = context.getConfiguration();

		Path trainingPath = new Path(conf.get(Util.CONF_DATASET_PATH) + "/"
				+ conf.get(Util.CONF_SESSION_DURATION) + "/"
				+ conf.get(Util.CONF_OPTIONS) + "/" + date);

		FileSystem fs = FileSystem.get(conf);

		trainingInstances = new TLinkedHashSet<Instance>(
				header.getNumClasses() / 2);

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
					Util.ping(context, CosimTestMapper.class);
					i = 0;
				}

				if (line == null) {
					break;
				}

				Instance instance;
				try {
					instance = Instance.fromString(header, line, context);
				} catch (Exception e) {
					LOG.warn("Skipping invalid instance: " + line);
					continue;
				}
				trainingInstances.add(instance);
			}
		}

		line = null;

		LOG.info("training day has " + trainingInstances.size()
				+ " classes/instances");
		Util.getMemoryInfo(CosimTestMapper.class);
	}

	@Override
	public void setup(Context context) {

		this.context = context;

		classIndex = ClassIndex.getInstance();

		Configuration conf = context.getConfiguration();

		float threshold = conf.getFloat(Util.CONF_MINIMUM_DF_OF_HOSTS, 0);

		try {

			Path headerPath = new Path(conf.get(Util.CONF_HEADER_PATH));

			LOG.info("Reading dataset header..." + headerPath.toString());

			header = new DataSetHeader(conf, headerPath);
			if (!classIndex.isPopulated()) {
				classIndex.init(conf);
				classIndex.populateIndex();
			}
			if (threshold > 0.0) {
				LOG.info("loading DF values");
				hostsWithMinimumDF = Util.getHostsWithDocumentFrequencies(conf,
						threshold);
			}

			LOG.info("loading training data...");

			loadTrainingInstances(conf.get(Util.CONF_TRAINING_DATE));

		} catch (Exception e) {
			LOG.error("setup failed with an exception!");
			e.printStackTrace();
			setupFailedException = e;
		}
	}
}
