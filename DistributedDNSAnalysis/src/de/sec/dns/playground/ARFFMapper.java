package de.sec.dns.playground;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.util.Util;

public class ARFFMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Log LOG = LogFactory.getLog(ARFFMapper.class);
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private Text k = new Text();
	private Text v = new Text();

	public ARFFMapper() {
		LOG.info("Mapper GenerateARFFFilesMap initialized");

	}

	@Override
	public void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {
		// 1267570240 526 f320625291c110f9bde6aeb9a7c2edfa_WOHNHEIM1
		// 49.233.199.132.in-addr.arpa PTR
		String[] rr = line.toString().split(" ");

		if (!Util.sanitize(rr[2], rr[3], rr[4])) {
			return;
		}

		if (rr.length < 3) {
			LOG.info(line);
		}
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(Long.parseLong(rr[0] + rr[1]));

		String date = dateFormat.format(c.getTime());

		// user@date
		k.set(rr[2] + "@" + date);
		v.set(rr[3]);
		context.write(k, v);
	}

	@Override
	public void setup(Context context) {

	}
}