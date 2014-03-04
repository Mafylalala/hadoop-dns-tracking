package de.sec.dns.synthesizer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import de.sec.dns.dataset.GenerateDataSetTool;

/**
 * The mapper for {@link GenerateDataSetTool}. It retrieves the users and hosts
 * from the log file and hands it to the combiner/reducer.
 * 
 * @author Christian Banse
 */
public class SynthesizeDataSetMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text k = new Text();
	private Text v = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		FileSplit file = (FileSplit) context.getInputSplit();

		// Extract date from path
		String date = file.getPath().getParent().getName();

		int i = value.toString().indexOf('}');

		k.set(value.toString().substring(i + 1));
		v.set(date + "_" + value);

		context.write(k, v);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

	}
}