package de.sec.dns.dataset;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class DataSetInputFormat extends
		FileInputFormat<LongWritable, ObjectWritable> {

	@Override
	public InstanceRecordReader createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new InstanceRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return true;
	}

}
