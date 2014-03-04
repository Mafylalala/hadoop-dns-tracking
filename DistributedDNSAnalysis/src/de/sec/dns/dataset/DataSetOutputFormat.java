package de.sec.dns.dataset;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import de.sec.dns.util.Util;

/**
 * An {@link OutputFormat} that writes data set files.
 * 
 * @author Christian Banse
 */
public class DataSetOutputFormat extends FileOutputFormat<Text, Text> {
	/**
	 * A {@link RecordWriter} that writes a line in the data set.
	 * 
	 * @author Christian Banse
	 */
	protected static class DataSetLineRecordWriter extends
			RecordWriter<Text, Text> {

		/**
		 * A byte array that holds a newline.
		 */
		private static final byte[] newline;

		/**
		 * UTF-8 encodings string.
		 */
		private static final String utf8 = "UTF-8";

		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		/**
		 * The current <i>TaskAttemptContext</i>.
		 */
		private TaskAttemptContext context;

		/**
		 * The <i>DataOutputStream<i> for the file we're writing in.
		 */
		private DataOutputStream out;

		/**
		 * Constructs a new <i>DataSetLineRecordWriter</i>
		 * 
		 * @param out
		 *            A <i>DataOutputStream</i>.
		 * @param context
		 *            The current <i>TaskAttemptContext</i>
		 */
		public DataSetLineRecordWriter(DataOutputStream out,
				TaskAttemptContext context) {
			this.out = out;
			this.context = context;
		}

		@Override
		public synchronized void close(TaskAttemptContext context)
				throws IOException {
			out.close();
		}

		@Override
		public synchronized void write(Text key, Text value) throws IOException {
			writeObject(value);
			Util.ping(context, DataSetOutputFormat.class);

			out.write(newline);
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            The object to print
		 * @throws IOException
		 *             if the write throws, we pass it on.
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}
	}

	/**
	 * The default extension for the data set file.
	 */
	public static String EXTENSION = ".dataset";

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();

		String extension = EXTENSION;
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);

		FSDataOutputStream fileOut = fs.create(file, false);

		return new DataSetLineRecordWriter(fileOut, job);
	}
}
