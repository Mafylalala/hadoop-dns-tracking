/**
 * =========================================================================
 * NaiveBayesDistributed - DoubleArrayWritable.java
 * =========================================================================
 * 
 * A multinomial naive bayes implementation for the hadoop environment. First
 * implemented....
 *
 */
package de.sec.dns.util;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

/**
 * A wrapper class for an ArrayWritable of DoubleWritables. This is needed by
 * the reducer.
 * 
 * @author Christian Banse
 */
public class DoubleArrayWritable extends ArrayWritable {

	/**
	 * The constructor used to create an instance of <i>DoubleArrayWritable</i>
	 */
	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}

	public DoubleArrayWritable(DoubleWritable[] doubles) {
		super(DoubleWritable.class);

		set(doubles);
	}
}