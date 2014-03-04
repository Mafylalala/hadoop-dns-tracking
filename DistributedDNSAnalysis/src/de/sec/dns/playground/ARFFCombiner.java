package de.sec.dns.playground;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.ardverk.collection.PatriciaTrie;
import org.ardverk.collection.StringKeyAnalyzer;
import org.ardverk.collection.Trie;

import de.sec.dns.util.Util;

public class ARFFCombiner extends Reducer<Text, Text, Text, Text> {
	Context context;

	long lastTime = System.currentTimeMillis();
	private Text v = new Text();

	@Override
	protected void reduce(Text userAtDate, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Trie<String, Integer> map = new PatriciaTrie<String, Integer>(
				StringKeyAnalyzer.INSTANCE);

		for (Text val : values) {
			try {
				Util.ping(context, ARFFCombiner.class);
				String host = val.toString();
				Integer count = map.get(host);
				if (count == null) {
					map.put(host, 1);
				} else {
					map.put(host, count + 1);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			Util.ping(context, ARFFCombiner.class);
		}

		for (String key : map.keySet()) {
			v.set(key + " " + map.get(key));

			context.write(userAtDate, v);
		}
		map = null;
	}

	@Override
	public void setup(Context context) {
		this.context = context;
	}
}
