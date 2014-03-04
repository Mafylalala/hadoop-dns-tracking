package de.sec.dns.cv;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.Instance;

/**
 * The Mapper for {@link CrossValidationDDFTool}. It reads in the
 * CrossValidationDataSet and passes the Data to the Reducer
 * 
 * @author Elmo Randschau
 */
public class CrossValidationDDFMapper extends
		Mapper<LongWritable, ObjectWritable, Text, Text> {
	/**
	 * A temporary variable used as mapreduce key.
	 */
	private Text k = new Text();

	/**
	 * A temporary variable used as mapreduce value.
	 */
	private Text v = new Text();

	@Override
	public void map(LongWritable key, ObjectWritable obj, Context context)
			throws IOException, InterruptedException {
		Instance instance;
		String classLabel;

		FileSplit file = (FileSplit) context.getInputSplit();

		// Extract date from path
		String date = file.getPath().getParent().getName();

		instance = (Instance) obj.get();
		classLabel = instance.getClassLabel();

		v.set(classLabel);

		for (int a = 0; a < instance.getNumValues(); a++) {
			long indexOfHost = instance.getIndex(a);

			k.set(DataSetHeader.DAILY_DOCUMENT_FREQUENCY_INDEX_DIRECTORY + " "
					+ indexOfHost + " " + date);

			context.write(k, v);
		}

	}
}
