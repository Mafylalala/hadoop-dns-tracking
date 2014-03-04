package de.sec.dns.dataset;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.ardverk.collection.PatriciaTrie;
import org.ardverk.collection.StringKeyAnalyzer;
import org.ardverk.collection.Trie;

import de.sec.dns.util.TextPair;
import de.sec.dns.util.Util;

/**
 * The reducer for {@link CachingTool}. It fetches entries from the mapper.
 * 
 * @author Elmo Randschau
 */
public class CachingReducer extends Reducer<TextPair, Text, NullWritable, Text> {

	public static enum CacheCounter { NEW_TO_CACHE, RENEW_CACHE, USED_FROM_CACHE };
	
	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private NullWritable k = NullWritable.get();

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(CachingReducer.class);

	Configuration conf;

	/**
	 * A Trie containing the host+type as Key and the start time of the
	 * caching-session
	 */
	Trie<String, Long> lastCachedTrie;

	/**
	 * The CachingDuration in ms
	 */
	Long _cachingDuration;

	@Override
	protected void reduce(TextPair pair, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		lastCachedTrie = new PatriciaTrie<String, Long>(
				StringKeyAnalyzer.INSTANCE);

		// debug counter
		int countNewToCache = 0;
		int countUseOldCache = 0;
		int countRenewCache = 0;

		for (Text line : values) {
			String[] entry = Util.veryFastSplit(line.toString(), ' ',
					Util.LOG_ENTRY_INDEX_REQUEST_TYPE + 1);

			String hostAndType = entry[Util.LOG_ENTRY_INDEX_HOST]
					+ entry[Util.LOG_ENTRY_INDEX_REQUEST_TYPE];

			Long time = Long.parseLong(entry[Util.LOG_ENTRY_INDEX_TIME_SECONDS]
					+ String.format(
							"%03d",
							Integer.parseInt(entry[Util.LOG_ENTRY_INDEX_TIME_MILLISECONDS])));

			if(time == null)
				throw new IOException("time is null in line "+line);
			
			Long lastSeen = lastCachedTrie.get(hostAndType);

			if (lastSeen == null) {
				// Host has not been seen so far
				countNewToCache++;
				context.getCounter(CacheCounter.NEW_TO_CACHE).increment(1);
				lastCachedTrie.put(hostAndType, time);
				context.write(k, line);
			} else {
				// host has been seen
				if ((lastSeen + _cachingDuration) < time) {
					// fixed cachingwindow
					// host cache-duration was been reached -> write to log
					// again
					countRenewCache++;
					context.getCounter(CacheCounter.RENEW_CACHE).increment(1);
					lastCachedTrie.put(hostAndType, time);
					context.write(k, line);
				} else if (((conf.get(Util.CONF_CACHING_SIMULATOR,
						Util.CONF_CACHING_FIXEDDURATION)
						.equals(Util.CONF_CACHING_SLIDINGWINDOW)))
						&& ((lastSeen + _cachingDuration) >= time)) {
					// sliding cachingwindow
					// host is requested during cachingduration -> update
					// cache-time
					// -> do not write to log file
					lastCachedTrie.put(hostAndType, time);
					countUseOldCache++;
					context.getCounter(CacheCounter.USED_FROM_CACHE).increment(1);
				} else {
					// host is used from cache -> do not write to log file
					countUseOldCache++;
					context.getCounter(CacheCounter.USED_FROM_CACHE).increment(1);
				}
			}
		}

		LOG.info(pair.getFirst().toString() + ":NewInCache: " + countNewToCache
				+ " RenewedInCache: " + countRenewCache + " UsedFromCache: "
				+ countUseOldCache);
	}

	@Override
	public void setup(Context context) {
		conf = context.getConfiguration();
		_cachingDuration = (long) (conf.getInt(Util.CONF_CACHING_DURATION,
				Util.DEFAULT_SESSION_DURATION)) * 60L * 1000L;
	}
}
