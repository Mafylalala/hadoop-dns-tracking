package de.sec.dns.dataset;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TLongSet;

import java.io.StreamTokenizer;
import java.io.StringReader;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import de.sec.dns.util.Util;

/**
 * This class represents one instance within a data set.
 * 
 * @author Christian Banse
 */
public class Instance {
	public static String ID_SEPARATOR = ",";

	/**
	 * Constructs a new instance from a <i>String</i>
	 * 
	 * @param dataset
	 *            The header of the data set the instance belongs to.
	 * @param line
	 *            The string that will be parsed.
	 * @param context
	 *            The current <i>TaskAttemptContext</i> of the job.
	 * @return The newly constructed <i>Instance<i>.
	 * @throws Exception
	 *             if an error occurs.
	 */
	public static Instance fromString(DataSetHeader dataset, String line,
			TaskAttemptContext context) throws Exception {

		// initialize the stream tokenizer
		StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(line));
		tokenizer.resetSyntax();

		// all other chars are word chars
		tokenizer.wordChars(' ' + 1, '\u00FF');

		// space and commas are whitespace
		tokenizer.whitespaceChars(0, ' ');
		tokenizer.whitespaceChars(',', ',');

		// set comment and quote chars
		tokenizer.commentChar('%');
		tokenizer.quoteChar('"');
		tokenizer.quoteChar('\'');

		// preserve } and {
		tokenizer.ordinaryChar('{');
		tokenizer.ordinaryChar('}');

		tokenizer.eolIsSignificant(true);

		// first token must be a number indicating the number of values
		tokenizer.nextToken();
		if (tokenizer.ttype != StreamTokenizer.TT_WORD) {
			throw new Exception("Invalid token, expected word.");
		}

		int numValues = Integer.parseInt(tokenizer.sval);
		int[] tmpIndices = new int[numValues];
		double[] tmpValues = new double[numValues];

		// next token must a {
		tokenizer.nextToken();
		if (tokenizer.ttype != '{') {
			throw new Exception("Invalid token, expected {.");
		}

		int pos = 0;
		do {
			// first token within the {} block must be a NUMBER indicating the
			// index or } if the line is at it's end
			tokenizer.nextToken();
			if (tokenizer.ttype == '}') {
				break;
			} else if (tokenizer.ttype != StreamTokenizer.TT_WORD) {
				throw new Exception("Invalid token, expected } or index");
			}

			tmpIndices[pos] = Integer.parseInt(tokenizer.sval);

			// next token must be a NUMBER indicating the value
			tokenizer.nextToken();
			if (tokenizer.ttype != StreamTokenizer.TT_WORD) {
				throw new Exception("Invalid token, expected value");
			}

			tmpValues[pos] = Double.parseDouble(tokenizer.sval);

			pos++;
			Util.ping(context, Instance.class);
		} while (true);

		// next token should be the class label
		tokenizer.nextToken();

		if (tokenizer.ttype != StreamTokenizer.TT_WORD) {
			throw new Exception("Invalid token, expected word");
		}

		// get the class label
		String classLabel = new String(tokenizer.sval);

		// next token should be the instance id
		tokenizer.nextToken();

		if (tokenizer.ttype != StreamTokenizer.TT_WORD) {
			throw new Exception("Invalid token, expected number");
		}

		// get the instance id
		String id = classLabel + ID_SEPARATOR
				+ Integer.parseInt(tokenizer.sval);

		// construct the new instance
		Instance instance = new Instance(dataset, tmpIndices, tmpValues,
				classLabel, id);

		tmpIndices = null;
		tmpValues = null;

		// LOG.info("successfully read instance of class " + classLabel);

		return instance;
	}

	/**
	 * The class label.
	 */
	private String classLabel;

	/**
	 * The header of the data set
	 */
	private DataSetHeader dataset;

	/**
	 * The attribute indices used by the instance.
	 */
	private int[] indices;

	/**
	 * The values used by the instance.
	 */
	private double[] values;

	/**
	 * The instance identifier within a class
	 */
	private String id;

	/**
	 * Constructs an instance from the given values.
	 * 
	 * @param dataset
	 *            The header of the data set this instance belongs to.
	 * @param indices
	 *            The attribute indices used by the instance.
	 * @param values
	 *            The values used by the instance.
	 * @param classLabel
	 *            The class label of the instance.
	 */
	private Instance(DataSetHeader dataset, int[] indices, double[] values,
			String classLabel, String id) {
		this.dataset = dataset;
		this.indices = new int[indices.length];
		this.values = new double[values.length];

		System.arraycopy(indices, 0, this.indices, 0, indices.length);
		System.arraycopy(values, 0, this.values, 0, values.length);

		this.classLabel = classLabel;
		this.id = id;
	}

	/**
	 * Returns the class label of this instance.
	 * 
	 * @return The class label.
	 */
	public String getClassLabel() {
		return classLabel;
	}

	/**
	 * Returns the index at the specified position.
	 * 
	 * @param position
	 *            The desired position.
	 * @return The index at the position.
	 */
	public int getIndex(int position) {
		return indices[position];
	}

	/**
	 * Returns the number of attributes.
	 * 
	 * @return The number of attributes.
	 */
	public int getNumAttributes() {
		return dataset.getNumAttributes();
	}

	/**
	 * Returns the number of classes.
	 * 
	 * @return The number of classes.
	 */
	public int getNumClasses() {
		return dataset.getNumClasses();
	}

	/**
	 * Returns the number of indices.
	 * 
	 * @return The number of classes.
	 */
	public int getNumIndices() {
		return indices.length;
	}

	/**
	 * Returns the number of values.
	 * 
	 * @return The number of values.
	 */
	public int getNumValues() {
		return values.length;
	}

	/**
	 * Returns the value at the specified position.
	 * 
	 * @param position
	 *            The desired position.
	 * @return The value at the specified position.
	 */
	public double getValue(int position) {
		return values[position];
	}

	/**
	 * Returns the instance identifier
	 * 
	 * @return The instance identifier.
	 */
	public String getId() {
		return id;
	}

	/**
	 * Removes all indices from the instance that are not contained in the list
	 * of allowed indices.
	 * 
	 * @param longSet
	 * 
	 */
	public void setAcceptableIndices(TLongSet acceptableIndices) {
		TIntArrayList remainingIndices = new TIntArrayList();
		TDoubleArrayList remainingValues = new TDoubleArrayList();
		for (int i = 0; i < indices.length; i++) {
			int index = indices[i];
			if (acceptableIndices.contains(index)) {
				remainingIndices.add(index);
				remainingValues.add(values[i]);
			}
		}

		this.indices = remainingIndices.toArray();
		this.values = remainingValues.toArray();
	}

	/**
	 * Returns the string representation of an instance as seen in the dataset
	 * files.
	 */
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append(getNumValues()).append(" { ");
		for (int i = 0; i < getNumValues(); i++) {
			b.append(indices[i]).append(" ").append(values[i]).append(" ");
		}
		b.append("} ").append(getId());
		return b.toString();
	}

}
