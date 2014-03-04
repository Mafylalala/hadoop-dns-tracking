/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.sec.dns.playground;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import de.sec.dns.util.ClassIndex;
import de.sec.dns.util.HostIndex;

/** An {@link OutputFormat} that writes plain text files. */
public class ARFFOutputFormat<K, V> extends FileOutputFormat<K, V> {
	protected static class ARFFLineRecordWriter<K, V> extends
			RecordWriter<K, V> {

		private static final byte[] newline;

		private static final String utf8 = "UTF-8";

		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		protected TaskAttemptContext context;

		long lastTime = System.currentTimeMillis();

		protected DataOutputStream out;

		public ARFFLineRecordWriter(DataOutputStream out,
				TaskAttemptContext context) {
			this.out = out;
			this.context = context;

			try {
				writeHeader();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public synchronized void close(TaskAttemptContext context)
				throws IOException {
			out.close();
		}

		public void ping() {
			final long currtime = System.currentTimeMillis();
			if (currtime - lastTime > 10000) {
				context.progress();
				lastTime = currtime;
			}
		}

		@Override
		public synchronized void write(K key, V value) throws IOException {

			// LOG.info("ARFFOUTPUTFORMAT WRITE");

			boolean nullKey = (key == null) || (key instanceof NullWritable);
			boolean nullValue = (value == null)
					|| (value instanceof NullWritable);
			if (nullKey || nullValue) {
				return;
			}

			// TODO: irgendwie gscheit machen
			writeObject(value);
			ping();

			out.write(newline);
		}

		public void writeHeader() throws Exception {
			ClassIndex classIndex = ClassIndex.getInstance();
			if (!classIndex.isPopulated()) {
				classIndex.init(context.getConfiguration());
				classIndex.populateIndex();
			}

			HostIndex hostIndex = HostIndex.getInstance();
			if (!hostIndex.isPopulated()) {
				hostIndex.init(context);
				hostIndex.populateIndex();
			}

			out.writeBytes("@relation arfftest\n");

			for (int i = 0; i < hostIndex.size(); i++) {
				out.writeBytes("@attribute " + hostIndex.getHostname(i)
						+ " numeric\n");
			}

			out.writeBytes("@attribute class {");
			for (int j = 0; j < classIndex.size(); j++) {
				out.writeBytes(classIndex.getClassname(j));
				if (j != classIndex.size() - 1) {
					out.writeBytes(",");
				}
			}

			out.writeBytes("}\n@data\n");
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
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

	public static String PART = FileOutputFormat.PART;

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();

		String extension = ".arff";
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);

		FSDataOutputStream fileOut = fs.create(file, false);

		return new ARFFLineRecordWriter<K, V>(fileOut, job);
	}
}
