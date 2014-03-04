package de.sec.dns.cv;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.sec.dns.dataset.Instance;

public class CrossValidationInterReducer extends
		Reducer<DoubleWritable, Text, Text, Text> {
	private HashMap<String, TIntHashSet> _userHost1 = new HashMap<String, TIntHashSet>();
	private HashMap<String, TIntHashSet> _userHost2 = new HashMap<String, TIntHashSet>();

	@Override
	protected void reduce(DoubleWritable one, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		// values: 2 \t 103_1,2 \t 36112
		for (Text line : values) {
			String user = line.toString().split("\t")[1]
					.split(Instance.ID_SEPARATOR)[0];
			int host = Integer.parseInt(line.toString().split("\t")[2]);
			int day = Integer.parseInt(line.toString().split("\t")[0]);

			TIntHashSet set;
			if (day == 1) {
				set = _userHost1.get(user);
				if (set == null) {
					set = new TIntHashSet();
					_userHost1.put(user, set);
				}
			} else {
				set = _userHost2.get(user);
				if (set == null) {
					set = new TIntHashSet();
					_userHost2.put(user, set);
				}
			}
			set.add(host);
		}
		context.write(new Text("USER"), new Text("intra" + "\t" + "inter"));
		for (Entry<String, TIntHashSet> e : _userHost1.entrySet()) {
			String currentUser = e.getKey();
			double intra = getIntra(currentUser);
			double inter = getInter(currentUser);
			intra = (double) ((int) (intra * 1000)) / 1000;
			inter = (double) ((int) (inter * 1000)) / 1000;

			context.write(
					new Text(currentUser),
					new Text(Double.toString(intra) + "\t"
							+ Double.toString(inter)));
		}
	}

	private double getInter(String currentUser) {
		int countTotal = 0;
		int countOnBothDays = 0;

		TIntIterator it = _userHost1.get(currentUser).iterator();
		while (it.hasNext()) {
			int i = it.next();
			countTotal++;
			for (Entry<String, TIntHashSet> e : _userHost2.entrySet()) {
				String userDay2 = e.getKey();
				if (userDay2.equals(currentUser))
					continue;
				TIntHashSet set = _userHost2.get(userDay2);
				if (set != null) {
					if (set.contains(i)) {
						countOnBothDays++;
						break;
					}
				}
			}
		}
		double res = (double) countOnBothDays / (double) countTotal;
		return res;
	}

	private double getIntra(String currentUser) {
		int countTotal = 0;
		int countOnBothDays = 0;

		TIntIterator it = _userHost1.get(currentUser).iterator();
		while (it.hasNext()) {
			int i = it.next();
			countTotal++;

			TIntHashSet set = _userHost2.get(currentUser);
			if (set != null) {
				if (set.contains(i)) {
					countOnBothDays++;
				}
			}
		}
		double res = (double) countOnBothDays / (double) countTotal;
		return res;
	}

}
