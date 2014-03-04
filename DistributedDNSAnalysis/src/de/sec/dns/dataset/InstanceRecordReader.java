package de.sec.dns.dataset;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import de.sec.dns.util.Util;

/**
 * This class reads an instance from an data set file record.
 * 
 * @author Christian Banse
 */
public class InstanceRecordReader extends
		RecordReader<LongWritable, ObjectWritable> {
	private static final Log LOG = LogFactory
			.getLog(InstanceRecordReader.class);

	private final static int MAX_LINE_LENGTH = Integer.MAX_VALUE;

	/**
	 * The current <i>TaskAttemptContext<i>.
	 */
	private TaskAttemptContext context;

	/**
	 * The header of the data set.
	 */
	private DataSetHeader dataset;

	/**
	 * The end position.
	 */
	private long end;

	/**
	 * The line reader.
	 */
	private LineReader in;

	/**
	 * The current key.
	 */
	private LongWritable key;

	/**
	 * The current position in the file.
	 */
	private long pos;

	/**
	 * The start position.
	 */
	private long start;

	/**
	 * The current value.
	 */
	private ObjectWritable value;

	@Override
	public void close() throws IOException {

	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public ObjectWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			// calculate the progress
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();

		this.context = context;
		Path headerPath = new Path(job.get(Util.CONF_HEADER_PATH));

		FileSystem fs = headerPath.getFileSystem(job);

		LOG.info("InstanceRecordReader reading dataset header..."
				+ headerPath.toString());

		dataset = new DataSetHeader(job, headerPath);

		start = split.getStart();
		end = start + split.getLength();

		final Path file = split.getPath();

		LOG.info("reading " + file + " from " + start + " to " + end);

		FSDataInputStream fileIn = fs.open(file);

		boolean skipFirstLine = false;
		if (start != 0) {
			skipFirstLine = true;
			--start;
			fileIn.seek(start);
		}
		in = new LineReader(fileIn, job);

		if (skipFirstLine) { // skip first line and re-establish "start".
			start += in.readLine(new Text(), 0,
					(int) Math.min(Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}

	@Override
	/**
	 * Fetches the next key/value pair. 
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException {
		Text tmp = new Text();

		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);

		if (value == null) {
			value = new ObjectWritable();
		}
		int newSize = 0;

		// loop through file while
		while (pos < end) {
			// read from file
			newSize = in.readLine(tmp, MAX_LINE_LENGTH, Math.max(
					(int) Math.min(Integer.MAX_VALUE, end - pos),
					MAX_LINE_LENGTH));

			if (newSize == 0) {
				break;
			}

			pos += newSize;

			Instance instance;
			try {
				// construct a new instance
				instance = Instance
						.fromString(dataset, tmp.toString(), context);
				if (instance == null) {
					key = null;
					value = null;
					return false;
				}
				value.set(instance);
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (newSize < MAX_LINE_LENGTH) {
				break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}
}
