package de.sec.dns.test;

import de.sec.dns.util.Util;

/**
 * An entry in the {@link ConfusionMatrix} which represents one class.
 * 
 * @author Christian Banse
 */
public class ConfusionMatrixEntry {
	/**
	 * The class label.
	 */
	private String classLabel;

	/**
	 * All instances that have been classified as this class.
	 */
	private String[] classifiedInstances;

	/**
	 * The false positive rate.
	 */
	private double fpRate;

	/**
	 * Number of classified instances.
	 */
	private int numTotalClassified;

	/**
	 * The true positive rate of the class.
	 */
	private double tpRate;

	/**
	 * The precision of the class.
	 */
	private double precision;

	/**
	 * Constructs an empty entry.
	 */
	public ConfusionMatrixEntry() {
		this.numTotalClassified = 0;

		this.tpRate = 0.0d;
		this.tpRate = 0.0d;

		this.classifiedInstances = null;
	}

	public ConfusionMatrixEntry(String line) {
		String[] rr = line.split("\t");

		// create a new entry
		classLabel = rr[Util.CONFUSION_MATRIX_CLASS];
		int numClassifications = Integer
				.parseInt(rr[Util.CONFUSION_MATRIX_NUMBER_OF_INSTANCES_ASSIGNED]);
		String[] tmp = new String[numClassifications];

		double tpRate = Double
				.parseDouble(rr[Util.CONFUSION_MATRIX_TRUE_POSITIVE_RATE]);
		double fpRate = Double
				.parseDouble(rr[Util.CONFUSION_MATRIX_FALSE_POSITIVE_RATE]);

		double precision = 0;
		// hotfix (doesn't affect future versions)
		if (!classLabel.equals("UNKNOWN")) {
			precision = Double.parseDouble(rr[Util.CONFUSION_MATRIX_PRECISION]);
		}

		// hotfix (doesn't affect future versions)
		if (classLabel.equals("UNKNOWN")
				&& rr.length < (Util.CONFUSION_MARTIX_FIRST_ASSIGNED_INSTANCE + numClassifications)) {
			System.arraycopy(rr,
					Util.CONFUSION_MARTIX_FIRST_ASSIGNED_INSTANCE - 1, tmp, 0,
					numClassifications);
		} else {
			System.arraycopy(rr, Util.CONFUSION_MARTIX_FIRST_ASSIGNED_INSTANCE,
					tmp, 0, numClassifications);
		}

		set(classLabel, numClassifications, tpRate, fpRate, precision, tmp);
	}

	/**
	 * Constructs an entry from the given values.
	 * 
	 * @param numTotalClassified
	 *            The number of classified instances.
	 * @param tpRate
	 *            The true positive rate.
	 * @param fpRate
	 *            The false positive rate.
	 * @param instances
	 *            The instances that have been classified as this class.
	 */
	public void set(String classLabel, int numTotalClassified, double tpRate,
			double fpRate, double precision, String[] instances) {
		this.numTotalClassified = numTotalClassified;
		this.tpRate = tpRate;
		this.fpRate = fpRate;
		this.precision = precision;
		this.classifiedInstances = instances;
	}

	/**
	 * Returns the instances which have been classified as this class.
	 * 
	 * @return The instances which have been classified as this class.
	 */
	public String[] getClassifiedInstances() {
		return classifiedInstances;
	}

	/**
	 * Returns the false positive rate.
	 * 
	 * @return The false positive rate.
	 */
	public double getFalsePositiveRate() {
		return fpRate;
	}

	/**
	 * Returns the number of classified instances.
	 * 
	 * @return The number of classified instances.
	 */
	public int getNumTotalClassified() {
		return numTotalClassified;
	}

	/**
	 * Returns the true positive rate.
	 * 
	 * @return The true positive rate.
	 */
	public double getTruePositiveRate() {
		return tpRate;
	}

	public double getRecall() {
		return getTruePositiveRate();
	}

	public double getPrecision() {
		return precision;
	}

	public String getClassLabel() {
		return classLabel;
	}
}