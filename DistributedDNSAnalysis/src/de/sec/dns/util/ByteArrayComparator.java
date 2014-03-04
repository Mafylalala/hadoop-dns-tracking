package de.sec.dns.util;

/**
 * Provides the ability to compare byte arrays.
 * 
 * @version $Id: $
 */
public class ByteArrayComparator {

	/*
	 * WTF !!! THIS TOOK 2 HRS TO DEBUG!!! NEVER COMPARE BYTES LIKE THAT!!!
	 * public static int compare( byte[] b1, byte[] b2 ) {
	 * 
	 * if ( b1.length != b2.length ) {
	 * 
	 * String msg = String.format( "differing lengths: %d vs %d", b1.length,
	 * b2.length );
	 * 
	 * throw new RuntimeException( msg ); }
	 * 
	 * for( int i = 0; i < b1.length; ++i ) {
	 * 
	 * if ( b1[i] < b2[i] ) return -1;
	 * 
	 * if ( b1[i] > b2[i] ) return 1;
	 * 
	 * if ( b1[i] == b2[i] ) {
	 * 
	 * //we're not done comparing yet. if ( i < b1.length - 1 ) continue;
	 * 
	 * return 0;
	 * 
	 * }
	 * 
	 * }
	 * 
	 * throw new RuntimeException();
	 * 
	 * }
	 */

	/*
	 * CODE borrowed from Hbase
	 * http://hbase.sourcearchive.com/documentation/0.20
	 * .4plus-pdfsg1-1/Bytes_8java-source.html#l01035 i.e. from Bytes.java from
	 * org.apache.hadoop.hbase.util;
	 */
	/**
	 * @param left
	 * @param right
	 * @return 0 if equal, < 0 if left is less than right, etc.
	 */
	public static int compareTo(final byte[] left, final byte[] right) {
		return compareTo(left, 0, left.length, right, 0, right.length);
	}

	/**
	 * @param b1
	 * @param b2
	 * @param s1
	 *            Where to start comparing in the left buffer
	 * @param s2
	 *            Where to start comparing in the right buffer
	 * @param l1
	 *            How much to compare from the left buffer
	 * @param l2
	 *            How much to compare from the right buffer
	 * @return 0 if equal, < 0 if left is less than right, etc.
	 */
	public static int compareTo(byte[] b1, int s1, int l1, byte[] b2, int s2,
			int l2) {
		// Bring WritableComparator code local
		int end1 = s1 + l1;
		int end2 = s2 + l2;
		for (int i = s1, j = s2; (i < end1) && (j < end2); i++, j++) {
			int a = (b1[i] & 0xff);
			int b = (b2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return l1 - l2;
	}

}