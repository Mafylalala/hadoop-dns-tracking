package de.sec.dns.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Source: <<<<<<< .working
 * http://code.google.com/p/flatmap/source/browse/trunk/src/java/com/spinn3r
 * /flatmap/FlatMap.java?spec=svn6&r=6 =======
 * http://code.google.com/p/flatmap/source/browse/trunk/src/java/com/spinn3r
 * /flatmap/FlatMap. java?spec=svn6&r=6 >>>>>>> .merge-right.r2524
 * 
 * TODO: how do we implement variable width values? Variable length keys are
 * fine as I can use truncated SHA1. Variable length data is FINE but I have to
 * allocate 4 bytes per entry for the pointer to the data section. I can get
 * around this problem by simply storing a DataPointer object in as the value
 * and adding this as an indirection to the underlying value.
 */
public class FlatMap {

	private static final Log LOG = LogFactory.getLog(FlatMap.class);

	// note, for fully in-memory buffers we can call ByteByffer.wrap()
	private byte[] bbuf = null;
	private final int OFFSET = 0;

	int size = 0;

	private final int valueSize = 8;

	public FlatMap(Path path, FileSystem fs) throws IOException {

		FSDataInputStream in = fs.open(path);
		LOG.info("Reading index from " + path.toString());
		int filesize = (int) fs.getFileStatus(path).getLen();
		LOG.info("Reported file size: " + filesize);
		bbuf = new byte[filesize];
		int totalReadBytes = 0;
		while (totalReadBytes < filesize) {
			int currentReadBytes = in.read(totalReadBytes, bbuf,
					totalReadBytes, filesize - totalReadBytes);
			LOG.info("Read bytes: " + currentReadBytes + " / " + filesize);
			totalReadBytes += currentReadBytes;
		}

		size = filesize / valueSize;

		LOG.info("Nmbr of elements in FlatMap: " + size);
	}

	public boolean containsValue(Object value) {

		for (int i = 0; i < size; ++i) {
			if (getValueFromPosition(i).equals(value)) {
				return true;
			}
		}

		return false;

	}

	/**
	 * Perform a binary search of the values in this flat map, return -1 if the
	 * value was not found, otherwise the value's index position .
	 * 
	 */
	public int get(byte[] value_data) {

		int low = 0;
		int high = size - 1;
		while (low <= high) {

			int mid = (low + high) >>> 1;
			byte[] midVal = getValueFromPosition(mid);
			int cmp = ByteArrayComparator.compareTo(midVal, value_data);
			if (cmp < 0) {
				low = mid + 1;
			} else if (cmp > 0) {
				high = mid - 1;
			} else {
				// value found
				return mid;
			}

		}
		return -1; // key not found

	}

	/**
	 * Given an offset, and a length, fetch the given blocks.
	 */
	private byte[] get(int offset, int length) {
		byte[] buff = new byte[length];

		// this is what ByteBuffer does internally anyway.
		for (int i = 0; i < length; ++i) {
			buff[i] = bbuf[offset + i];
		}

		return buff;
	}

	public byte[] getValueFromPosition(int pos) {

		int offset = OFFSET + (pos * valueSize);

		byte[] data = get(offset, valueSize);

		return data;

	}

	public boolean isEmpty() {
		return size == 0;
	}

	public int size() {
		return size;
	}

}
