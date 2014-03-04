package de.sec.dns.analysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.dataset.GenerateDataSetTool;
import de.sec.dns.util.Util;

/**
 * The mapper for {@link GenerateDataSetTool}. It retrieves the users and hosts
 * from the log file and hands it to the combiner/reducer.
 * 
 * @author Christian Banse
 */
public class MobileMeIdentifierMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text k = new Text();

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

	@Override
	public void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {

		// 1267570240 526 1_1 49.233.199.132.in-addr.arpa PTR
		String[] rr = Util.veryFastSplit(line.toString(), ' ',
				Util.LOG_ENTRY_INDEX_REQUEST_TYPE + 1);

		String user = rr[Util.LOG_ENTRY_INDEX_USER];
		String host = rr[Util.LOG_ENTRY_INDEX_HOST];

		// we're only interested in MobileMe hosts
		if (!host.contains("members.mac.com")) {
			return;
		}

		// special preparation for user names that have a . in them!
		host = host.replace("\\.", "_");
		System.out.println(host);

		String[] domains = Util.splitAndReverse(host);

		// domains = { com, mac, members, <mobilemeusername>, ... }
		if (domains.length < 4) {
			return;
		}

		String username = domains[3];

		if (username.equals("_tcp")) {
			return;
		}

		// store all relevant user names for a user
		k.set(user);
		v.set(username);
		context.write(k, v);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	}
}