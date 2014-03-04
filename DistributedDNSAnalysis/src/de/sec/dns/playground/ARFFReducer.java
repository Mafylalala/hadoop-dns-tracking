package de.sec.dns.playground;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.ardverk.collection.PatriciaTrie;
import org.ardverk.collection.StringKeyAnalyzer;
import org.ardverk.collection.Trie;

import com.sun.org.apache.xml.internal.security.utils.Base64;

import de.sec.dns.util.CompactCharSequence;
import de.sec.dns.util.HostIndex;
import de.sec.dns.util.Util;

public class ARFFReducer extends Reducer<Text, Text, Text, Text> {
	private static int counter = 0;
	private static final Log LOG = LogFactory.getLog(ARFFReducer.class);

	private MessageDigest digest;
	private HostIndex hostIndex = HostIndex.getInstance();
	private Text k = new Text();

	long lastTime = System.currentTimeMillis();

	private MultipleOutputs<Text, Text> m;
	private Text v = new Text();

	@Override
	public void cleanup(Context context) {
		try {
			m.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getHexString(byte[] b) throws Exception {
		String result = "";
		for (int i = 0; i < b.length; i++) {
			result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
		}
		return result;
	}

	public int getHostIndex(CompactCharSequence host) throws IOException {
		return hostIndex.getIndexPosition(host);
	}

	@Override
	protected void reduce(Text userAtDate, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		CompactCharSequence md5Host = null;
		String host = null;
		int value = 0;

		counter++;
		if ((counter % 100) == 0) {
			LOG.info("REDUCER: reduced " + counter + " records; current: "
					+ userAtDate);
		}

		Trie<String, Integer> map = new PatriciaTrie<String, Integer>(
				StringKeyAnalyzer.INSTANCE);

		int valuecount = 0;

		for (Text val : values) {
			try {
				Util.ping(context, ARFFReducer.class);
				String[] rr = val.toString().split(" ");
				host = rr[0];

				// kommt direkt vom mapper
				if (rr.length == 1) {
					value = 1;
					// kommt vom reducer (host value)
				} else {
					value = Integer.parseInt(rr[1]);
				}

				Integer count = map.get(host);

				if (count == null) {
					map.put(host, value);
				} else {
					map.put(host, count + value);
				}
				valuecount++;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// user@date
		String[] rr = userAtDate.toString().split("@");
		String user = rr[0];
		String date = rr[1];

		HashMap<Integer, Double> map2 = new HashMap<Integer, Double>();
		for (String key : map.keySet()) {
			Util.ping(context, ARFFReducer.class);

			try {
				md5Host = new CompactCharSequence(Base64.encode(digest
						.digest(key.getBytes())));
			} catch (Exception e) {
				e.printStackTrace();
			}

			int i = getHostIndex(md5Host);

			double freq = map.get(key);
			if (i > -1) {
				map2.put(i, freq);
			}
		}

		StringBuilder builder = new StringBuilder();
		builder.append("{ ");

		ArrayList<Integer> list = new ArrayList<Integer>(map2.keySet());
		Collections.sort(list);

		for (Integer i : list) {
			builder.append(i).append(" ");
			builder.append(map2.get(i)).append(", ");
		}

		builder.append(hostIndex.size() + " " + user + "}");
		v.set(builder.toString());

		m.write(k, v, date + "/data");
		builder = null;

		map = null;

		if ((counter % 1000) == 0) {
			Util.getMemoryInfo(ARFFReducer.class);
		}

	}

	@Override
	public void setup(Context context) {
		try {
			this.digest = MessageDigest.getInstance("MD5");

			if (!hostIndex.isPopulated()) {
				hostIndex.init(context);
				hostIndex.populateIndex();
			}

			LOG.info("Reducer GenerateARFFFilesReduce initialized");

			m = new MultipleOutputs<Text, Text>(context);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
