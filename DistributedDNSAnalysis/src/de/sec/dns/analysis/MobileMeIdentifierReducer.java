package de.sec.dns.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.sec.dns.util.Util;

public class MobileMeIdentifierReducer extends Reducer<Text, Text, Text, Text> {
	HashMap<String, Boolean> names = new HashMap<String, Boolean>();

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text v = new Text();

	@Override
	public void cleanup(Context context) {
	}

	@SuppressWarnings("unchecked")
	public <K, V> Map.Entry<K, V> getGreatestValue(HashMap<K, V> map) {
		Map.Entry<K, V> maxEntry = null;

		for (Map.Entry<K, V> entry : map.entrySet()) {
			if ((maxEntry == null)
					|| (((Comparable<V>) entry.getValue()).compareTo(maxEntry
							.getValue()) > 0)) {
				maxEntry = entry;
			}
		}

		return maxEntry;
	}

	public String getLongestValue(ArrayList<String> list) {
		String longestEntry = null;

		for (String entry : list) {
			if ((longestEntry == null)
					|| (entry.length() > longestEntry.length())) {
				longestEntry = entry;
			}
		}

		return longestEntry;
	}

	public String[] guessName(String username) throws Exception {
		// get rid of all numbers
		username = username.replaceAll("[0-9]", "");

		String firstName = null;
		String lastName = null;

		username = username.toLowerCase();

		ArrayList<String> possibleMatches = new ArrayList<String>();

		for (String name : names.keySet()) {
			if (username.contains(name.toLowerCase())) {
				// add to possible match list
				possibleMatches.add(name.toLowerCase());
			}
		}

		// find the longest matching first name
		if (possibleMatches.size() > 0) {
			firstName = getLongestValue(possibleMatches);
		}

		// if we find a underscore there is a high chance that the underscore
		// splits the first from the last name
		String[] rr = username.split("_");
		if (rr.length > 1) {
			lastName = rr[rr.length - 1];
			if ((firstName == null) && (rr[0].length() > 3)) {
				firstName = rr[0];
			}
		} else {
			// no underscore but we did find a first name!
			// so there is a chance that the first name is followed by the last
			// name
			if (firstName != null) {
				int pos = username.indexOf(firstName);
				if (pos >= 0) {
					pos += firstName.length();

					// OK, seems we're at the end of the user name so maybe the
					// user name was <last name><first name>!
					if (pos == username.length()) {
						lastName = username.substring(0,
								pos - firstName.length());
					} else {
						lastName = username.substring(pos);
					}
				}
			}
		}

		// less than 4 letters seems implausible for a German last name
		if ((lastName != null) && (lastName.length() < 4)) {
			lastName = null;
		}

		return new String[] { firstName, lastName };
	}

	@Override
	protected void reduce(Text user, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		HashMap<String, Integer> userNames = new HashMap<String, Integer>();

		for (Text value : values) {
			Integer i = userNames.get(value.toString());

			if (i == null) {
				i = new Integer(0);
			}
			userNames.put(value.toString(), i + 1);
			System.out.println(value);
		}

		// get most likely entry
		Map.Entry<String, Integer> entry = getGreatestValue(userNames);

		// make a guess about the name
		String[] name = new String[] { null, null };

		try {
			name = guessName(entry.getKey());
		} catch (Exception ex) {
			// this could very well happen due out of bounds exception and
			// stuff, but just ignore it
			ex.printStackTrace();
		}

		v.set(entry.getKey() + "\t" + name[0] + "\t" + name[1]);

		context.write(new Text(""), v);
	}

	@Override
	public void setup(Context context) {
		context.getConfiguration().set("this.is.temporary",
				"/user/masterarbeit/names.txt");

		Util.populateBlackOrWhiteList(context.getConfiguration(), names,
				"this.is.temporary");
	}
}
