package de.sec.dns.test;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.util.Util;

/**
 * The mapper class for {@link ConfusionMatrixTool}.
 * 
 * @author Christian Banse
 */
public class ConfusionMatrixMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Set<String> nonPredictedClasses;

	@Override
	public void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {

		String rr[] = Util.veryFastSplit(line.toString(), '\t', 3);

		String instanceId = rr[0];

		// String rr2[] = Util.veryFastSplit(instanceId, ',', 2);
		// String instanceClass = rr2[1];

		String classifiedAs = rr[1];
		String cosim = rr[2];

		// this instance has been classified as 'classifiedAs',
		// so add it to the values of the 'classifiedAs' class.
		// NEW: add cosim for the pair for our reject option
		context.write(new Text(classifiedAs), new Text(instanceId + "\t"
				+ cosim));

		nonPredictedClasses.remove(classifiedAs);
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		// Loop through all non predicted classes and add them to the confusion
		// matrix with no assigned
		// instances
		for (String clazz : nonPredictedClasses) {
			context.write(new Text(clazz), new Text());
		}
	}

	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		String session = conf.get(Util.CONF_TRAINING_DATE);
		nonPredictedClasses = OccurenceMatrix.getOccurenceData(context, conf,
				session).keySet();
	}
}