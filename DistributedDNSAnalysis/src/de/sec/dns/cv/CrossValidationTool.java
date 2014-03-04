package de.sec.dns.cv;

import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.sec.dns.analysis.AveragePrecisionRecallTool;
import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.FilterDataSetTool;
import de.sec.dns.test.ConfusionMatrixTool;
import de.sec.dns.test.CosimTestTool;
import de.sec.dns.test.JaccardTestTool;
import de.sec.dns.test.TestTool;
import de.sec.dns.test.YangTestTool;
import de.sec.dns.training.CandidatePatternTool;
import de.sec.dns.training.TrainingTool;
import de.sec.dns.training.YangTrainingTool;
import de.sec.dns.util.Util;

/**
 * 
 * @author Christian Banse
 * 
 */
public class CrossValidationTool extends Configured implements Tool {
	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "CrossValidation";

	private static int NUM_OF_REDUCE_TASKS = 1;

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		String jobName = Util.JOB_NAME + " [" + CrossValidationTool.ACTION
				+ "]";

		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		// delete old cross validation directory
		FileSystem fs = FileSystem.get(conf);
		Path basePath = new Path(conf.get(Util.CONF_BASE_PATH));
		Path cvPath = new Path(basePath + "/" + Util.CROSS_VALIDATION_PATH);

		conf.set(Util.CONF_CROSS_VALIDATION_PATH, cvPath.toString());

		fs.delete(cvPath, true);

		// first we need to split the dataset
		if (ToolRunner.run(conf, new CrossValidationSplitTool(), null) == 0) {
			return 0;
		}

		// set the header path to our new header
		Util.showStatus("Creating new dataset header...");

		// reading old header
		DataSetHeader dh = new DataSetHeader(conf, basePath.suffix("/header"));
		int numAttributes = dh.getNumAttributes();

		Path headerPath = cvPath.suffix("/header");
		conf.set(Util.CONF_HEADER_PATH, headerPath.toString());

		// write the new header
		int numClasses = conf.getInt(Util.CONF_CROSS_VALIDATION_NUM_OF_CLASSES,
				1);
		int numFolds = conf.getInt(Util.CONF_CROSS_VALIDATION_NUM_FOLDS, 10);
		int numInstancesPerClass = conf.getInt(
				Util.CONF_CROSS_VALIDATION_NUM_INSTANCES_PER_CLASS, 10);

		// write the counters to the metadata file
		FSDataOutputStream out = fs.create(headerPath.suffix("/"
				+ DataSetHeader.META_DATA_FILE));
		PrintWriter w = new PrintWriter(out);

		w.println("relationName=cv" + numFolds);
		w.println("numAttributes=" + numAttributes);
		w.println("numClasses=" + numClasses);
		w.close();

		out = fs.create(headerPath.suffix("/"
				+ DataSetHeader.NUM_INSTANCES_DIRECTORY + "/header-r-00000"));
		w = new PrintWriter(out);

		w.println("2010-01-01-00-00\t" + (numInstancesPerClass / numFolds)
				* (numFolds - 1) * numClasses);
		w.println("2010-01-02-00-00\t" + (numInstancesPerClass / numFolds) * 1
				* numClasses);
		w.close();

		Util.showStatus("Copying indices...");

		// copy the host index
		Path srcHostIndex = basePath.suffix("/header/"
				+ DataSetHeader.HOST_INDEX_DIRECTORY);
		Path dstHostIndex = headerPath.suffix("/"
				+ DataSetHeader.HOST_INDEX_DIRECTORY);

		FileUtil.copy(fs, srcHostIndex, fs, dstHostIndex, false, conf);

		// copy the class index, use the occurence matrix for this purpose
		// because it contains only
		// the selected classes
		Path srcClassIndex = basePath.suffix("/header/"
				+ DataSetHeader.HOST_INDEX_DIRECTORY);
		Path dstClassIndex = headerPath.suffix("/"
				+ DataSetHeader.HOST_INDEX_DIRECTORY);

		FileUtil.copy(fs, srcClassIndex, fs, dstClassIndex, false, conf);

		// copy occurrence matrix
		Path srcMatrix = basePath.suffix("/header/"
				+ DataSetHeader.OCCURRENCE_MATRIX_DIRECTORY);
		Path dstMatrix = headerPath.suffix("/"
				+ DataSetHeader.OCCURRENCE_MATRIX_DIRECTORY);

		FileUtil.copy(fs, srcMatrix, fs, dstMatrix, false, conf);

		Util.showStatus("Starting cross validation with " + numFolds
				+ " folds ...");

		// TODO: make generic session names possible
		conf.set(Util.CONF_FIRST_SESSION, "2010-01-01-00-00");
		conf.set(Util.CONF_LAST_SESSION, "2010-01-02-00-00");

		conf.set(Util.CONF_TRAINING_DATE, "2010-01-01-00-00");
		conf.set(Util.CONF_TEST_DATE, "2010-01-02-00-00");

		Path analysisPath = cvPath.suffix("/analysis/summary");
		conf.set(Util.CONF_ANALYSIS_PATH, analysisPath.toString());

		Job job = new Job(conf, jobName);

		for (int i = 0; i < numFolds; i++) {
			Util.showStatus("Setting dataset to split #" + i + "...");
			Path datasetPath = cvPath.suffix("/dataset/split" + i);
			conf.set(Util.CONF_DATASET_PATH, datasetPath.toString());

			Path trainingPath = cvPath.suffix("/training/split" + i);
			conf.set(Util.CONF_TRAINING_PATH, trainingPath.toString());

			Path candidatePatternPath = cvPath.suffix("/candidatepattern/split"
					+ i);
			conf.set(Util.CONF_CANDIDATEPATTERN_PATH,
					candidatePatternPath.toString());

			Path interIntraPath = cvPath.suffix("/interintra/split" + i);
			conf.set(Util.CONF_INTERINTRA_PATH, interIntraPath.toString());

			Path filterDataSetPath = cvPath.suffix("/filtered_dataset/split"
					+ i);
			conf.set(Util.CONF_DATASET_FILTER_PATH,
					filterDataSetPath.toString());

			// train on CV training instances
			/*
			 * if (ToolRunner.run(conf, new TrainingTool(), null) == 0) { throw
			 * new Exception("Training failed"); }
			 */
			if ((conf.get(Util.CONF_CLASSIFIER)).equals("lift")
					|| (conf.get(Util.CONF_CLASSIFIER)).equals("support")
					|| conf.getBoolean(Util.CONF_DATASET_FILTER, false)
					|| conf.getBoolean(Util.CONF_INTERINTRA, false)) {
				if (ToolRunner.run(conf, new CandidatePatternTool(), null) == 0) {
					throw new Exception("CandidatePattern creation failed");
				}
			}

			if (conf.getBoolean(Util.CONF_INTERINTRA, false)) {
				if (ToolRunner.run(conf, new CrossValidationInterTool(), null) == 0) {
					throw new Exception("CrossValidationInter creation failed");
				}
			}

			// if (conf.getBoolean(Util.CONF_USE_DAILY_DOCUMENT_FREQUENCIES,
			// false)) {
			if (conf.get(Util.CONF_CLASSIFIER).equals("lift")
					|| conf.get(Util.CONF_CLASSIFIER).equals("support")) {
				if (ToolRunner.run(conf, new CrossValidationDDFTool(), null) == 0) {
					throw new Exception(
							"CrossValidationDailyDocumentFrequency creation failed");
				}
			}

			if (conf.getBoolean(Util.CONF_DATASET_FILTER, false)) {
				// run the FilterDataSetTool
				if (ToolRunner.run(conf, new FilterDataSetTool(), null) == 0) {
					return 0;
				}
			}
			if ((conf.get(Util.CONF_CLASSIFIER)).equals("lift")
					|| (conf.get(Util.CONF_CLASSIFIER)).equals("support")) {
				if (ToolRunner.run(conf, new YangTrainingTool(), null) == 0) {
					throw new Exception("Training failed");
				}
			}
			// training tool ist auch bei jaccard nötig, weil die confusion
			// matrix die
			// daten aus dem training tool braucht, um richtig zu rechnen
			else if ((conf.get(Util.CONF_CLASSIFIER)).equals("mnb")
					|| ((conf.get(Util.CONF_CLASSIFIER)).equals("jaccard"))
					|| ((conf.get(Util.CONF_CLASSIFIER)).equals("cosim"))) {
				if (ToolRunner.run(conf, new TrainingTool(), null) == 0) {
					throw new Exception("Training failed");
				}
			}
			// End of Training

			Path testPath = cvPath.suffix("/test/split" + i);
			Path matrixPath = cvPath.suffix("/matrix/split" + i);
			conf.set(Util.CONF_TEST_PATH, testPath.toString());
			conf.set(Util.CONF_MATRIX_PATH, matrixPath.toString());

			// Test with Test Classes
			/*
			 * if (ToolRunner.run(conf, new TestTool(), null) == 0) { return 0;
			 * }
			 */
			if ((conf.get(Util.CONF_CLASSIFIER)).equals("lift")
					|| (conf.get(Util.CONF_CLASSIFIER)).equals("support")) {
				if (ToolRunner.run(conf, new YangTestTool(), null) == 0) {
					break;
				}
			}

			else if ((conf.get(Util.CONF_CLASSIFIER)).equals("mnb")) {
				if (ToolRunner.run(conf, new TestTool(), null) == 0) {
					break;
				}
			}

			else if ((conf.get(Util.CONF_CLASSIFIER)).equals("jaccard")) {
				if (ToolRunner.run(conf, new JaccardTestTool(), null) == 0) {
					break;
				}
			}

			else if ((conf.get(Util.CONF_CLASSIFIER)).equals("cosim")) {
				if (ToolRunner.run(conf, new CosimTestTool(), null) == 0) {
					break;
				}
			}
			// End of Test

			if (ToolRunner.run(conf, new ConfusionMatrixTool(), null) == 0) {
				return 0;
			}

			analysisPath = cvPath.suffix("/analysis/split" + i);
			conf.set(Util.CONF_ANALYSIS_PATH, analysisPath.toString());

			if (ToolRunner.run(conf, new AveragePrecisionRecallTool(), null) == 0) {
				return 0;
			}

			// add the created average precision / recall data to our summary
			// job
			FileInputFormat.addInputPath(job,
					analysisPath.suffix("/averagePrecisionRecallPerUser"));
		}

		// set the number of reducers
		job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

		job.setJarByClass(CrossValidationTool.class);
		job.setMapperClass(CrossValidationMapper.class);
		job.setReducerClass(CrossValidationReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		analysisPath = cvPath.suffix("/analysis/summary");
		conf.set(Util.CONF_ANALYSIS_PATH, analysisPath.toString());

		FileOutputFormat.setOutputPath(job,
				analysisPath.suffix("/averagePrecisionRecallPerUser"));

		Util.showStatus("Running " + jobName);

		return job.waitForCompletion(true) ? 1 : 0;
	}
}
