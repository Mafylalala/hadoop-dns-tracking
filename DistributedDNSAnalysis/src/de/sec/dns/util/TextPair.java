package de.sec.dns.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Partitioner;

public class TextPair implements WritableComparable<TextPair> {
	public static final class Comparator extends WritableComparator {
		protected Comparator() {
			super(TextPair.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	public static final class FirstComparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPERATOR = new Text.Comparator();

		protected FirstComparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstLength1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstLength2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);

				return TEXT_COMPERATOR.compare(b1, s1, firstLength1, b2, s2,
						firstLength2);
			} catch (IOException ex) {
				throw new IllegalArgumentException(ex);
			}
		}
	}

	public static final class LastComparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPERATOR = new Text.Comparator();

		protected LastComparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstLength1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstLength2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);

				int secondLength1 = WritableUtils.decodeVIntSize(b1[s1
						+ firstLength1])
						+ readVInt(b1, s1 + firstLength1);
				int secondLength2 = WritableUtils.decodeVIntSize(b2[s2
						+ firstLength2])
						+ readVInt(b2, s2 + firstLength2);

				return TEXT_COMPERATOR.compare(b1, s1 + firstLength1,
						secondLength1, b2, s2 + firstLength2, secondLength2);
			} catch (IOException ex) {
				throw new IllegalArgumentException(ex);
			}
		}
	}

	public static final class FirstPartitioner extends
			Partitioner<TextPair, Text> {

		@Override
		public int getPartition(TextPair key, Text value, int numReduceTasks) {
			return (key.first.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	Text first;

	Text second;

	public TextPair() {
		first = new Text();
		second = new Text();
	}

	public TextPair(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public int compareTo(TextPair pair) {
		int cmp = getFirst().compareTo(pair.getFirst());
		if (cmp != 0) {
			return cmp;
		}
		return getSecond().compareTo(pair.getSecond()); // reverse
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public void set(String first, String second) {
		this.first.set(first.getBytes());
		this.second.set(second.getBytes());
	}

	public void setFirst(String s) {
		first.set(s.getBytes());
	}

	public void setSecond(String s) {
		second.set(s.getBytes());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
}
