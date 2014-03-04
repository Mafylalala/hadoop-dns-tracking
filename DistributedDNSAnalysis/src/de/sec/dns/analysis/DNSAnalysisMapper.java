package de.sec.dns.analysis;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.sec.dns.test.ConfusionMatrix;
import de.sec.dns.test.ConfusionMatrixEntry;
import de.sec.dns.test.OccurenceMatrix;
import de.sec.dns.util.Util;

/**
 * The mapper class used by {@link DNSAnalysisTool}. It condenses information
 * gathered by the ConfusionMatrix such as the true positive or false positive
 * rate in a character symbol specified in {@link SymbolCounter}.
 * 
 * @author Christian Banse
 */
public class DNSAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
	public enum SymbolCounter {
		ALL('A'), COLON(':'), HASH('#'), MINUS('-'), PIPE('|'), PLUS('+'), UNDERSCORE(
				'_'), UNKNOWN('?');

		/**
		 * Char representation of the symbol.
		 */
		private final char symbol;

		/**
		 * The constructor for a new symbol.
		 * 
		 * @param symbol
		 *            The char representation of the symbol.
		 */
		SymbolCounter(char symbol) {
			this.symbol = symbol;
		}

		/**
		 * Returns the char representation of the symbol.
		 * 
		 * @return The char representation of the symbol.
		 */
		public char getSymbol() {
			return this.symbol;
		}

		@Override
		public String toString() {
			return "" + this.symbol;
		}
	}

	/**
	 * A logger.
	 */
	private static Log LOG = LogFactory.getLog(DNSAnalysisMapper.class);

	private ArrayList<String> randomSessions = new ArrayList<String>();

	// moved to OccurenceMatrix
	// public static OccurenceMatrix getOccurenceData(Context context,
	// Configuration conf, String session)

	/**
	 * The confusion matrices for all possible sessions.
	 */
	private HashMap<String, ConfusionMatrix> confusionMatrices = new HashMap<String, ConfusionMatrix>();

	/**
	 * Time information about the first session.
	 */
	private Calendar firstSession;

	/**
	 * A temporary variable used for the mapreduce key.
	 */
	private Text key = new Text();

	/**
	 * Time information about the last session.
	 */
	private Calendar lastSession;

	/**
	 * The occurence matrices for all possible sessions.
	 */
	private HashMap<String, OccurenceMatrix> occurenceMatrices = new HashMap<String, OccurenceMatrix>();

	/**
	 * Time information about the current test session.
	 */
	private Calendar testSession;

	/**
	 * Time information about the current training session.
	 */
	private Calendar trainingSession;

	/**
	 * A temporary variable used for the mapreduce value.
	 */
	private Text value = new Text();

	private boolean OMNISCIENT_CLASSIFIER;

	public void doAnalysis(StringBuilder builder, String classLabel,
			Context context, IntWritable longestPlus, IntWritable plus) {
		// user did not occur on training session
		if (!occurenceMatrices.get(Util.formatDate(trainingSession.getTime()))
				.containsKey(classLabel)) {
			// skip this session
			builder.append(" ");

			return;
		}
		
		SymbolCounter symbol = SymbolCounter.UNKNOWN;
		
		if(OMNISCIENT_CLASSIFIER) { // Pretend that we always make correct predictions
			
			if(occurenceMatrices.get(
					Util.formatDate(testSession.getTime())).containsKey(
					classLabel)) {
				symbol = SymbolCounter.PLUS;
				context.getCounter(symbol).increment(1);
				plus.set(plus.get() + 1); // start the chain of plus'es
			} else {
				symbol = SymbolCounter.PIPE;
				context.getCounter(symbol).increment(1);
				// the chain of plus'es has come to an end
				if (longestPlus.compareTo(plus) < 0) {
					longestPlus.set(plus.get());
				}
				plus.set(0);
			}
		
		} else { // Proceed with the actual classification results

			
			
			// get the ConfusionMatrixEntry for the test session
			ConfusionMatrixEntry entry = confusionMatrices.get(
					Util.formatDate(testSession.getTime())).get(classLabel);
	
			// true positive rate is 1 and false positive rate is 0. only one
			// instance
			// has been classified for this class and it was the correct one.
			// very good!
			if ((entry.getTruePositiveRate() == 1)
					&& (entry.getFalsePositiveRate() == 0)) {
				symbol = SymbolCounter.PLUS;
				context.getCounter(symbol).increment(1);
				plus.set(plus.get() + 1); // start the chain of plus'es
			}
	
			// true positive rate is 1 and false positive rate is above 0. one
			// instance
			// has been correctly classified but also some other incorrect
			// instances
			// have been classified as this class. still good but not as good as
			// the case above
			if ((entry.getTruePositiveRate() == 1)
					&& (entry.getFalsePositiveRate() > 0)) {
				symbol = SymbolCounter.HASH;
				context.getCounter(symbol).increment(1);
				// the chain of plus'es has come to an end
				if (longestPlus.compareTo(plus) < 0) {
					longestPlus.set(plus.get());
				}
				plus.set(0);
			}
	
			// true positive rate is 0, false positive rate is above 0 and
			// total classified instances is greater than one. pretty bad.
			// several incorrect instance
			// have been classified as this class
			if ((entry.getTruePositiveRate() == 0)
					&& (entry.getFalsePositiveRate() > 0)
					&& (entry.getNumTotalClassified() > 1)) {
				symbol = SymbolCounter.MINUS;
				context.getCounter(symbol).increment(1);
				// the chain of plus'es has come to an end
				if (longestPlus.compareTo(plus) < 0) {
					longestPlus.set(plus.get());
				}
				plus.set(0);
			}
	
			// same as above but only one incorrect instance has been classified
			// as this class.
			// this is extremely bad because the attacker has no chance of
			// detecting an error
			if ((entry.getTruePositiveRate() == 0)
					&& (entry.getFalsePositiveRate() > 0)
					&& (entry.getNumTotalClassified() == 1)) {
				symbol = SymbolCounter.UNDERSCORE;
				context.getCounter(symbol).increment(1);
				// the chain of plus'es has come to an end
				if (longestPlus.compareTo(plus) < 0) {
					longestPlus.set(plus.get());
				}
				plus.set(0);
			}
	
			// the classifier predicted that the user was not active in the test
			// session.
			// no instance has been classified as this class. in fact the user
			// was active
			// in the test session, so the classifier was wrong
			if ((entry.getTruePositiveRate() == 0)
					&& (entry.getFalsePositiveRate() == 0)
					&& occurenceMatrices
							.get(Util.formatDate(testSession.getTime()))
							.containsKey(classLabel)) {
				symbol = SymbolCounter.COLON;
				context.getCounter(symbol).increment(1);
				// the chain of plus'es has come to an end
				if (longestPlus.compareTo(plus) < 0) {
					longestPlus.set(plus.get());
				}
				plus.set(0);
			}
	
			// the classifier correctly predicted that the user was not active
			// in the test session
			if ((entry.getTruePositiveRate() == 0)
					&& (entry.getFalsePositiveRate() == 0)
					&& !occurenceMatrices.get(
							Util.formatDate(testSession.getTime())).containsKey(
							classLabel)) {
				symbol = SymbolCounter.PIPE;
				context.getCounter(symbol).increment(1);
				// the chain of plus'es has come to an end
				if (longestPlus.compareTo(plus) < 0) {
					longestPlus.set(plus.get());
				}
				plus.set(0);
			}
		}

		context.getCounter(DNSAnalysisMapper.SymbolCounter.ALL).increment(1);

		// jump to the next session
		builder.append(symbol);
	}

	@Override
	public void map(LongWritable l, Text line, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();

		// get the class label
		String classLabel = line.toString().split("\t")[0];
		StringBuilder builder = new StringBuilder();

		builder.append("/");

		// some utility variables to determine the longest chain of plus'es
		IntWritable longestPlus = new IntWritable(0);
		IntWritable plus = new IntWritable(0);

		if ((conf.getInt(Util.CONF_USE_RANDOM_TRAINING_SESSIONS, 0)) == 0) {
			// start with the first session
			trainingSession = (Calendar) firstSession.clone();

			// iterate over all sessions
			while (trainingSession.before(lastSession)) {
				testSession = (Calendar) trainingSession.clone();
				testSession.add(
						Calendar.MINUTE,
						context.getConfiguration().getInt(
								Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
								Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST));
				if (Util.isDataAvailableForTrainingAndTest(conf,
						trainingSession, testSession)) {
					LOG.info("Analyzing: "
							+ Util.formatDate(trainingSession.getTime())
							+ " -> " + Util.formatDate(testSession.getTime()));
					doAnalysis(builder, classLabel, context, longestPlus, plus);
				}

				trainingSession.add(
						Calendar.MINUTE,
						context.getConfiguration().getInt(
								Util.CONF_SESSION_DURATION,
								Util.DEFAULT_SESSION_DURATION));
			}
		} else {
			/*
			 * int seed = context.getConfiguration().getInt(
			 * Util.CONF_RANDOM_TRAINING_SESSIONS_SEED, 0);
			 * 
			 * ArrayList<String> usedDates = new ArrayList<String>();
			 * 
			 * Random generator = new Random(); generator.setSeed(seed);
			 */

			/*
			 * int sessionDuration = conf.getInt(Util.CONF_SESSION_DURATION,
			 * Util.DEFAULT_SESSION_DURATION);
			 */
			int sessionOffset = conf.getInt(
					Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
					Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST);
			/*
			 * int max = (int) ((lastSession.getTimeInMillis() - firstSession
			 * .getTimeInMillis()) / (1000 * 60)) - sessionOffset;
			 */

			for (String session : randomSessions) {
				// for (int i = 0; i < randomDays; i++) {
				/*
				 * do { int timeLapsed = (generator.nextInt(max) /
				 * sessionDuration) sessionDuration; trainingSession =
				 * (Calendar) firstSession.clone();
				 * trainingSession.add(Calendar.MINUTE, timeLapsed); } while
				 * (usedDates.contains(Util.formatDate(trainingSession
				 * .getTime())));
				 */

				try {
					trainingSession = Util.getCalendarFromString(session);
				} catch (ParseException e) {
					e.printStackTrace();
				}

				testSession = (Calendar) trainingSession.clone();
				testSession.add(Calendar.MINUTE, sessionOffset);

				conf.set(Util.CONF_TRAINING_DATE,
						Util.formatDate(trainingSession.getTime()));
				conf.set(Util.CONF_TEST_DATE,
						Util.formatDate(testSession.getTime()));

				doAnalysis(builder, classLabel, context, longestPlus, plus);
			}
		}

		// now we're really at the end of the chain
		if (longestPlus.compareTo(plus) < 0) {
			longestPlus.set(plus.get());
		}

		// print out the chain of plus'es
		builder.append("/ " + longestPlus);

		key.set(classLabel);
		value.set(builder.toString());

		// write the string to the reducer
		context.write(key, value);
	}

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String sessionTime = null;
		String testSessionTime = null;

		try {
			OMNISCIENT_CLASSIFIER = context.getConfiguration().getBoolean(
					Util.CONF_OMNISCIENT_CLASSIFIER, Util.DEFAULT_OMNISCIENT_CLASSIFIER);

			
			firstSession = Util.getCalendarFromString(conf
					.get(Util.CONF_FIRST_SESSION));
			lastSession = Util.getCalendarFromString(conf
					.get(Util.CONF_LAST_SESSION));

			int randomDays = 0;

			if ((randomDays = conf.getInt(
					Util.CONF_USE_RANDOM_TRAINING_SESSIONS, 0)) == 0) {

				// if offset-between-training-and-test is used to evaluate
				// offsets of e.g. 2880hrs, the
				// to-Option will have to end accordingly earlier than the end
				// of the dataset.
				// problem is: the confusion matrix needs the trainingdata for
				// all test days, even for the ones
				// behind the to-Option. So we add the offet to the last
				// training session to make sure that the training also happens
				// for the remaining sessions.
				// this is also a problem for the default offset of 1440 - in
				// earlier versions
				// there was special handling to make sure that the "last"
				// session was fed to training
				// we handle this special case with the following approach, too.
				Calendar lastSessionForLoading = (Calendar) lastSession.clone();
				lastSessionForLoading.add(Calendar.MINUTE, conf.getInt(
						Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
						Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST));

				trainingSession = (Calendar) firstSession.clone();

				Util.showStatus("loading Occurence and Confusion Matrices ...");

				// iterate over all sessions
				while (trainingSession.before(lastSessionForLoading)) {
					testSession = (Calendar) trainingSession.clone();
					testSession.add(Calendar.MINUTE, conf.getInt(
							Util.CONF_SESSION_DURATION,
							Util.DEFAULT_SESSION_DURATION));

					sessionTime = Util.formatDate(trainingSession.getTime());
					testSessionTime = Util.formatDate(testSession.getTime());

					if (Util.isDataAvailableForTrainingAndTest(conf,
							trainingSession, trainingSession /* !! */)) {
						LOG.info("loading OccurenceMatrix from session "
								+ sessionTime + "... (training)");

						// load OccurenceMatrix

						try {
							OccurenceMatrix occMatrix = OccurenceMatrix
									.getOccurenceData(context, conf,
											sessionTime);
							occurenceMatrices.put(
									Util.formatDate(trainingSession.getTime()),
									occMatrix);
						} catch (IOException e) {
							LOG.info("NOT loaded OccurenceMatrix for session "
									+ sessionTime
									+ "... (training) -- no data found");
						}

					}

					// load ConfusionMatrix
					String confusionMatrixPath = conf
							.get(Util.CONF_MATRIX_PATH)
							+ "/"
							+ conf.get(Util.CONF_SESSION_DURATION)
							+ "/"
							+ conf.get(Util.CONF_OPTIONS)
							+ "/"
							+ testSessionTime + "/part-r-00000";

					if (FileSystem.get(conf).exists(
							new Path(confusionMatrixPath))) {

						LOG.info("loading ConfusionMatrix from session "
								+ testSessionTime + "... (test)");

						confusionMatrices.put(testSessionTime,
								new ConfusionMatrix(FileSystem.get(conf),
										confusionMatrixPath));

					} else {
						LOG.info("NOT loading ConfusionMatrix from session "
								+ testSessionTime + "... (test) -- "
								+ confusionMatrixPath + "missing");
					}

					// jump to the next session
					trainingSession.add(Calendar.MINUTE, conf.getInt(
							Util.CONF_SESSION_DURATION,
							Util.DEFAULT_SESSION_DURATION));
				}

				// probably not needed any more
				if (Util.isDataAvailableForTrainingAndTest(conf,
						trainingSession, trainingSession)) {
					// read the OccurenceMatrix from the last session
					sessionTime = Util.formatDate(trainingSession.getTime());

					LOG.info("loading OccurenceMatrix from session "
							+ sessionTime + "... (last Session)");

					try {
						OccurenceMatrix occMatrix = OccurenceMatrix
								.getOccurenceData(context, conf, sessionTime);
						occurenceMatrices.put(
								Util.formatDate(trainingSession.getTime()),
								occMatrix);
					} catch (IOException e) {
						LOG.info("NOT loaded OccurenceMatrix for session "
								+ sessionTime
								+ "... (training) -- no data found");
					}
				}

			} else {
				int seed = context.getConfiguration().getInt(
						Util.CONF_RANDOM_TRAINING_SESSIONS_SEED, 0);

				LOG.info("Using seed: " + seed);

				Random generator = new Random(seed);

				int sessionDuration = conf.getInt(Util.CONF_SESSION_DURATION,
						Util.DEFAULT_SESSION_DURATION);
				int sessionOffset = conf.getInt(
						Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
						Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST);
				int max = (int) ((lastSession.getTimeInMillis() - firstSession
						.getTimeInMillis()) / (1000 * 60)) - sessionOffset;

				for (int i = 0; i < randomDays; i++) {
					do {
						int timeLapsed = (generator.nextInt(max) / sessionDuration)
								* sessionDuration;
						trainingSession = (Calendar) firstSession.clone();
						trainingSession.add(Calendar.MINUTE, timeLapsed);
					} while (randomSessions.contains(Util
							.formatDate(trainingSession.getTime())));

					testSession = (Calendar) trainingSession.clone();
					testSession.add(Calendar.MINUTE, sessionOffset);

					conf.set(Util.CONF_TRAINING_DATE,
							Util.formatDate(trainingSession.getTime()));
					conf.set(Util.CONF_TEST_DATE,
							Util.formatDate(testSession.getTime()));

					sessionTime = Util.formatDate(trainingSession.getTime());
					testSessionTime = Util.formatDate(testSession.getTime());

					LOG.info("loading OccurenceMatrix from session "
							+ sessionTime + "...");

					// load OccurenceMatrix
					occurenceMatrices.put(Util.formatDate(trainingSession
							.getTime()), OccurenceMatrix.getOccurenceData(
							context, conf, sessionTime));

					LOG.info("loading ConfusionMatrix from session "
							+ testSessionTime + "...");

					// load ConfusionMatrix
					confusionMatrices.put(
							testSessionTime,
							new ConfusionMatrix(FileSystem.get(conf), conf
									.get(Util.CONF_MATRIX_PATH)
									+ "/"
									+ conf.get(Util.CONF_SESSION_DURATION)
									+ "/"
									+ conf.get(Util.CONF_OPTIONS)
									+ "/"
									+ testSessionTime + "/part-r-00000"));

					// load OccurenceMatrix from testSession
					LOG.info("loading OccurenceMatrix from session "
							+ testSessionTime + "...");

					// load OccurenceMatrix
					occurenceMatrices.put(testSessionTime, OccurenceMatrix
							.getOccurenceData(context, conf, testSessionTime));

					randomSessions.add(Util.formatDate(trainingSession
							.getTime()));
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
