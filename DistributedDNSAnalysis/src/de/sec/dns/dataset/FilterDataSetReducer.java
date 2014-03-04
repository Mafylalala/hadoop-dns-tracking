package de.sec.dns.dataset;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.util.Util;

public class FilterDataSetReducer extends Reducer<Text, Text, Text, Text> {
	Text k = new Text();
	Text v = new Text();

	/**
	 * Used to handle multiple output files.
	 */
	private MultipleOutputs<Text, Text> m;

	@Override
	protected void reduce(Text date_option, Iterable<Text> instances,
			Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Path headerPath = new Path(conf.get(Util.CONF_HEADER_PATH));
		DataSetHeader header = new DataSetHeader(conf, headerPath);

		String date = date_option.toString().split(" ")[0];

		// path to the option used in the original DataSet. 'raw' or 'tfn'
		String option_path = date_option.toString().split(" ")[1];

		Instance outputInstance = null;

		for (Text instance : instances) {
			try {
				// create a new Instance to make sure we output a valid Instance
				outputInstance = Instance.fromString(header,
						instance.toString(), context);
			} catch (Exception e) {
				e.printStackTrace();
			}
			k.set(date);
			// v.set(instance);
			v.set(outputInstance.toString());

			m.write(k, v, option_path + "/" + date + "/data");
		}
	}

	@Override
	public void setup(Context context) {
		// initialize the multiple outputs handler
		m = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	public void cleanup(Context context) {
		try {
			// close the multiple output handler
			m.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
