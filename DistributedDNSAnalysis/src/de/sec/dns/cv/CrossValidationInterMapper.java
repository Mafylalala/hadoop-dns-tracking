package de.sec.dns.cv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import de.sec.dns.util.Util;

public class CrossValidationInterMapper extends
		Mapper<LongWritable, Text, DoubleWritable, Text> {

	@Override
	public void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		// line = 103_1,2 36112
		Text v;
		FileSplit file = (FileSplit) context.getInputSplit();
		String date = file.getPath().getName().split("-r")[0];
		if (date.equals(conf.get(Util.CONF_TRAINING_DATE))) {
			v = new Text("1" + "\t" + line.toString());
		} else {
			v = new Text("2" + "\t" + line.toString());
		}

		context.write(new DoubleWritable(1), v);
	}

}
