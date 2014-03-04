package de.sec.dns.dataset;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.ardverk.collection.PatriciaTrie;
import org.ardverk.collection.StringKeyAnalyzer;
import org.ardverk.collection.Trie;

import de.sec.dns.util.TextPair;
import de.sec.dns.util.Util;

/**
 * The combiner for {@link GenerateDataSetTool}. It fetches entries from the
 * mapper and starts combining the entries.
 * 
 * @author Christian Banse
 */
public class GenerateDataSetCombiner extends
		Reducer<TextPair, Text, TextPair, Text> {
	private Text v = new Text();

	@Override
	protected void reduce(TextPair userAtDate, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Trie<String, Integer> map = new PatriciaTrie<String, Integer>(
				StringKeyAnalyzer.INSTANCE);

		// iterate over all values and store their occurence in a Trie/HashMap
		for (Text val : values) {
			try {
				Util.ping(context, GenerateDataSetCombiner.class);
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
			Util.ping(context, GenerateDataSetCombiner.class);
		}

		for (String key : map.keySet()) {
			v.set(key + " " + map.get(key));

			context.write(userAtDate, v);
		}
		map = null;
	}
}
