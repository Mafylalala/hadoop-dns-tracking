package de.sec.dns.dataset;

import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The reducer class for {@link TrainingTool}. It sums all the occurence tuples
 * and calculates the bayesian probability.
 * 
 * @author Christian Banse
 */
public class CountEffectiveNumOfAttributesReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory
			.getLog(CountEffectiveNumOfAttributesReducer.class);

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<Text, DoubleWritable> m;

	private TLongHashSet allHostnames = new TLongHashSet();
	private TLongHashSet hostnamesOfCurrentInstance = new TLongHashSet();

	// contains all the individual hostname counts per instance; will be sorted
	// in cleanup to print out
	// percentiles.
	private TIntLinkedList allEffectiveNumOfHostnamesPerInstance = new TIntLinkedList();

	/**
	 * A temporary variable for the mapreduce value.
	 */
	private Text k = new Text();
	private DoubleWritable v = new DoubleWritable();

	@Override
	public void cleanup(Context context) {
		try {
			// close the multiple output handler
			k.set("effectiveNumberOfAttributesOverall");
			v.set(allHostnames.size());
			m.write(k, v, "statisticsOverall");

			// postprocess individual session data
			allEffectiveNumOfHostnamesPerInstance.sort();

			int numEntries = allEffectiveNumOfHostnamesPerInstance.size();
			int min = allEffectiveNumOfHostnamesPerInstance.get(0);
			int q25 = allEffectiveNumOfHostnamesPerInstance
					.get((int) (numEntries * 0.25));
			int q50 = allEffectiveNumOfHostnamesPerInstance
					.get((int) (numEntries * 0.50));
			int q75 = allEffectiveNumOfHostnamesPerInstance
					.get((int) (numEntries * 0.75));
			int max = allEffectiveNumOfHostnamesPerInstance
					.get(allEffectiveNumOfHostnamesPerInstance.size() - 1);
			float sum = allEffectiveNumOfHostnamesPerInstance.sum();
			float avg = sum / (float) numEntries;

			k.set("effectiveAttributesPerInstanceMin");
			v.set(min);
			m.write(k, v, "statisticsOverall");

			k.set("effectiveAttributesPerInstanceQ25");
			v.set(q25);
			m.write(k, v, "statisticsOverall");

			k.set("effectiveAttributesPerInstanceQ50");
			v.set(q50);
			m.write(k, v, "statisticsOverall");

			k.set("effectiveAttributesPerInstanceQ75");
			v.set(q75);
			m.write(k, v, "statisticsOverall");

			k.set("effectiveAttributesPerInstanceMax");
			v.set(max);
			m.write(k, v, "statisticsOverall");

			k.set("effectiveAttributesPerInstanceAvg");
			v.set(avg);
			m.write(k, v, "statisticsOverall");

			m.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(Text instance, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {

		final String numqueriesFlag = "NUMQUERIES_";
		if(instance.toString().startsWith(numqueriesFlag)) {
			String userAtDate = instance.toString().substring(numqueriesFlag.length());
			// iterate over all hostname indices per instance
			double sumNumQueries = 0;
			for (DoubleWritable numQueries : values) { // there should be only 1 numqueries value per instance; iterate over that nevertheless
				// add hostname to total set
				sumNumQueries += numQueries.get();
			}
			k.set(userAtDate.toString());
			v.set(sumNumQueries);
			m.write(k, v, "numQueriesPerInstance");
			return;
		}
		
		// iterate over all hostname indices per instance
		for (DoubleWritable index : values) {
			// add hostname to total set
			double d = index.get();
			hostnamesOfCurrentInstance.add((long) d);
			allHostnames.add((long) d);
		}

		k.set(instance.toString());
		int size = hostnamesOfCurrentInstance.size();
		v.set(size);

		m.write(k, v, "attributesPerInstance");

		allEffectiveNumOfHostnamesPerInstance.add(size);

		// clear set for current instance because it contains stuff from last
		// instance
		hostnamesOfCurrentInstance.clear();
	}

	@Override
	public void setup(Context context) {
		try {
			// initialize the multiple outputs handler
			m = new MultipleOutputs<Text, DoubleWritable>(context);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
