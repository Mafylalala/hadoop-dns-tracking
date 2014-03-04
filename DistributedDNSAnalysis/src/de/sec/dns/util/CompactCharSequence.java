package de.sec.dns.util;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

/**
 * Source: http://www.javamex.com/tutorials/memory/ascii_charsequence.shtml
 * 
 * @author Christian Banse
 */
public class CompactCharSequence implements CharSequence, Serializable,
		Comparable<CompactCharSequence> {
	private static final String ENCODING = "ISO-8859-1";

	static final long serialVersionUID = 1L;
	private final byte[] data;
	private final int end;
	private final int offset;

	public CompactCharSequence(byte[] data) {
		this(data, 0, data.length);
	}

	private CompactCharSequence(byte[] data, int offset, int end) {
		this.data = data;
		this.offset = offset;
		this.end = end;
	}

	public CompactCharSequence(String str) {
		try {
			data = str.getBytes(ENCODING);
			offset = 0;
			end = data.length;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unexpected: " + ENCODING
					+ " not supported!");
		}
	}

	@Override
	public char charAt(int index) {
		int ix = index + offset;
		if (ix >= end) {
			throw new StringIndexOutOfBoundsException("Invalid index " + index
					+ " length " + length());
		}
		return (char) (data[ix] & 0xff);
	}

	@Override
	public int compareTo(CompactCharSequence o) {
		int len1 = end;
		int len2 = o.end;
		int n = Math.min(len1, len2);
		byte v1[] = data;
		byte v2[] = o.data;
		int i = offset;
		int j = o.offset;

		if (i == j) {
			int k = i;
			int lim = n + i;
			while (k < lim) {
				byte c1 = v1[k];
				byte c2 = v2[k];
				if (c1 != c2) {
					return c1 - c2;
				}
				k++;
			}
		} else {
			while (n-- != 0) {
				byte c1 = v1[i++];
				byte c2 = v2[j++];
				if (c1 != c2) {
					return c1 - c2;
				}
			}
		}
		return len1 - len2;
	}

	public byte[] getBytes() {
		return data;
	}

	@Override
	public int length() {
		return end - offset;
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		if ((start < 0) || (end >= (this.end - offset))) {
			throw new IllegalArgumentException("Illegal range " + start + "-"
					+ end + " for sequence of length " + length());
		}
		return new CompactCharSequence(data, start + offset, end + offset);
	}

	@Override
	public String toString() {
		try {
			return new String(data, offset, end - offset, ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unexpected: " + ENCODING
					+ " not supported");
		}
	}

}
