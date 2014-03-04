package de.sec.dns.analysis;

import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.DisplaySetting;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.util.HelpFormatter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.sec.dns.cv.CrossValidationTool;
import de.sec.dns.dataset.CachingTool;
import de.sec.dns.dataset.CountEffectiveNumOfAttributesTool;
import de.sec.dns.dataset.DailyDocumentFrequencyTool;
import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.FilterDataSetTool;
import de.sec.dns.dataset.GenerateDataSetHeaderTool;
import de.sec.dns.dataset.GenerateDataSetTool;
import de.sec.dns.dataset.GenerateNGramsTool;
import de.sec.dns.dataset.OverallDocumentFrequencyTool;
import de.sec.dns.dataset.RangeQueriesSimulatorTool;
import de.sec.dns.synthesizer.SynthesizeDataSetTool;
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
 * This <i>Tool</i> combines all the functionality of the DistributedDNSAnalysis
 * program. It sole purpose is to call all the other hadoop <i>Tool</i>s based
 * on the user's input.
 * 
 * @author Christian Banse, Dominik Herrmann, Elmo Randschau
 */
public class DNSAnalysisTool2 extends Configured implements Tool {
	/**
	 * The action used by the job name.
	 */
	private static String ACTION = "Analysis";

	/**
	 * An enum class used as an hadoop counter group. It specifies all possible
	 * symbols the analysis tool can print out.
	 * 
	 * @author Christian Banse
	 */
	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory.getLog(DNSAnalysisTool2.class);

	/**
	 * The number of reduce tasks. We set this to one so we only have one
	 * analysis output file.
	 */
	private static int NUM_OF_REDUCE_TASKS = 1;

	/**
	 * The main entry point if this class is used as an application.
	 * 
	 * @param args
	 *            The command line arguments.
	 * @throws Exception
	 *             if an error occurs.
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * if(args.length < 8) { System.out.println(
		 * "USAGE: output_path input_path raw|tfn first_session last_session session_duration "
		 * +
		 * "generate_dataset_header? generate_dataset? do_test_training? do_analysis? "
		 * +
		 * "[minimum_host_occurence_frequency] [minimum_requests_per_profile] [maximum_requests_per_profile] "
		 * + "[add_second_lvl_domains?] "+
		 * "[size_of_dummy_pool] [generate_static_dummies?] [number_of_dummies_per_request] "
		 * + "[dummy_host_path] [minimum_df_of_hosts]"); System.out.println("");
		 * System.out.println(
		 * "- first_session and last_session has to be in the format yyyy-MM-dd-HH-mm."
		 * ); System.out.println("- session_duration is to be set in minutes.");
		 * } else {
		 */
		// just pass all command line arguments to the tool
		ToolRunner.run(new DNSAnalysisTool2(), args);
		// }
	}

	Option numClassesOption;
	Option numInstancesPerClassOption;
	Option numTrainingInstancesPerClassOption;
	Option seedOption;
	Group doCrossValidationChildren;
	Option doCrossValidationOption;
	Option doOpenWorldSimulationOption;

	/**
	 * The main entry point if this class is called as a {@link Tool}.
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		DefaultOptionBuilder oBuilder = new DefaultOptionBuilder();
		ArgumentBuilder aBuilder = new ArgumentBuilder();
		GroupBuilder gBuilder = new GroupBuilder();

		Option basePathOption = oBuilder
				.withShortName("p")
				.withDescription(
						"Specifies the base path where all the computational files are or will be stored")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withRequired(true).create();

		Option numCoresOption = oBuilder.withLongName("num-cores")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withDescription("Number of CPU cores per cluster node")
				.withRequired(false).create();

		Option numNodesOption = oBuilder.withLongName("num-nodes")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withDescription("Number of cluster nodes").withRequired(false)
				.create();

		Option sessionDurationOption = oBuilder
				.withShortName("s")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withDescription(
						"Represents the duration of a user session in minutes")
				.withRequired(true).create();

		Option sessionOffsetOption = oBuilder
				.withShortName("o")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withDescription(
						"Represents the offset of a user session from 00:00 in minutes")
				.withRequired(false).create();

		Option modeOption = oBuilder.withShortName("m")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withDescription("Sets the transformation mode: raw|tfn")
				.withRequired(true).create();

		Option rawDataPathOption = oBuilder
				.withLongName("input-dir")
				.withDescription(
						"Specifies the path where the raw log files reside")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withRequired(true).create();

		Option userWhiteListPath = oBuilder
				.withLongName("user-whitelist-path")
				.withDescription(
						"Specifies a file which contains a user whitelist")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option hostWhiteListPath = oBuilder
				.withLongName("host-whitelist-path")
				.withDescription(
						"Specifies a file which contains a host whitelist")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option userBlackListPath = oBuilder
				.withLongName("user-blacklist-path")
				.withDescription(
						"Specifies a file which contains a user blacklist")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option hostBlackListPath = oBuilder
				.withLongName("host-blacklist-path")
				.withDescription(
						"Specifies a file which contains a host blacklist")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		/*
		 * Option hostIndexOption = oBuilder .withLongName("hostindex")
		 * .withDescription("Specifies the path to the hostindex-bin file")
		 * .withArgument(aBuilder .withMinimum(1) .withMaximum(1) .create())
		 * .withRequired(true) .create();
		 */

		Option writeHostIndexMapping = oBuilder
				.withLongName("write-host-index-mapping")
				.withDescription(
						"Write out the mapping between hostnames and hostIndex indices to header/hostIndexMapping")
				.create();

		Option generateHeaderOnly = oBuilder.withLongName("header-only")
				.withDescription("Only generates the dataset header").create();

		Option skipHeader = oBuilder.withLongName("skip-header")
				.withDescription("Skips the dataset header generation")
				.create();

		Option skipNgrams = oBuilder.withLongName("skip-ngrams")
				.withDescription("Skips the ngram generation").create();

		Option skipDailyDocumentFrequencies = oBuilder
				.withLongName("skip-daily-document-frequencies")
				.withDescription(
						"Skips the daily document frequencies generation")
				.create();

		Option addAllStaticOption = oBuilder
				.withLongName("include-all-static-users")
				.withDescription(
						"includes all users with static IPs in the dataset (otherwise only users from the dorm networksare included)")
				.create();

		Option addSecondLvlDomainsOption = oBuilder
				.withLongName("add-2nd-level-domains")
				.withDescription(
						"Determines whether 2nd level domains should be added to dataset")
				.create();

		Option addOnlyTypeAOption = oBuilder
				.withLongName("only-type-a-and-aaaa")
				.withDescription(
						"include only requests of type A or AAAA, drop all others")
				.create();

		Option addIgnoreSophosxlOption = oBuilder
				.withLongName("ignore-sophosxl")
				.withDescription(
						"ignore all requests for hostnames ending with sophosxl.com")
				.create();

		Option lowercaseHostnamesOption = oBuilder
				.withLongName("lowercase-hostnames")
				.withDescription("Lowercases all hostnames").create();

		Option rejectInvalidHostnamesOption = oBuilder
				.withLongName("reject-invalid-hostnames")
				.withDescription("Rejects invalid hostnames").create();

		Option rejectWindowsBrowserQueries = oBuilder
				.withLongName("reject-windows-browser-queries")
				.withDescription(
						"Rejects queries created by the network browser of Windows (HOSTNAME.uni-regensburg.de in our case)")
				.create();

		Option minimumHostDFOption = oBuilder
				.withLongName("minimum-host-df")
				.withDescription(
						"The minimum document frequency a considered host must have")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option minimumHostOccurenceOption = oBuilder
				.withLongName("minimum-host-occurence")
				.withDescription(
						"The minimum occurence a considered host must have")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option minimumRequestsPerUserOption = oBuilder
				.withLongName("minimum-requests-per-user")
				.withDescription("The minimum a considered user must have")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option maximumRequestsPerUserOption = oBuilder
				.withLongName("maximum-requests-per-user")
				.withDescription("The maximum a considered user must have")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option useDynamicRequestRangeIndexOption = oBuilder
				.withLongName("use-dynamic-request-range-index")
				.withDescription(
						"Uses the fancy dynamic request range index. It's magic")
				.create();

		Option dynamicRequestRangeSkipOnlyTopUsersOption = oBuilder
				.withLongName("dynamic-request-range-skip-only-top-users")
				.withDescription("Skips only the most active users. It's magic")
				.create();

		Option dynamicRequestRangeSkipOnlyBottomUsersOption = oBuilder
				.withLongName("dynamic-request-range-skip-only-bottom-users")
				.withDescription(
						"Skips only the least active users. It's magic")
				.create();

		Option numberOfDummySamplesOption = oBuilder
				.withLongName("number-of-dummy-samples")
				.withDescription(
						"Specifies the number of dummy samples to be chosen from the pool")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option numberOfDummiesPerRequestOption = oBuilder
				.withLongName("number-of-dummies-per-request")
				.withDescription("Specifies the number of dummies per request")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option dummyHostPathOption = oBuilder.withLongName("dummy-host-path")
				.withDescription("Specifies the path of the dummy hosts file")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option generateStaticDummiesOption = oBuilder
				.withLongName("generate-static-dummies")
				.withDescription(
						"Type of range queries; set to true to always generate the "
								+ "same dummies for a given Host. set to false to generate "
								+ "totally random dummies for each occurrence of a given host")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Group useRangeQuerySimulatorChildren = gBuilder
				.withOption(numberOfDummySamplesOption)
				.withOption(numberOfDummiesPerRequestOption)
				.withOption(dummyHostPathOption)
				.withOption(generateStaticDummiesOption).create();

		Option useRangeQuerySimulatorOption = oBuilder
				.withLongName("use-range-queries-simulator")
				.withDescription("Uses the range queries simulator")
				.withChildren(useRangeQuerySimulatorChildren).create();

		Option skipNMostPopularHostnamesOption = oBuilder
				.withLongName("skip-n-most-popular-hostnames")
				.withDescription(
						"Skips the supplied argument of most popular hostnames")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option onlyNMostPopularHostnamesOption = oBuilder
				.withLongName("only-n-most-popular-hostnames")
				.withDescription(
						"Only uses the supplied argument of most popular hostnames")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option ignoreFrequenciesOption = oBuilder
				.withLongName("ignore-frequencies")
				.withDescription("Ignore host frequencies").create();

		Option dailyDocumentFrequencies = oBuilder
				.withLongName("use-daily-document-frequencies")
				.withDescription("Calculate and use daily document frequencies")
				.create();

		Option overallDocumentFrequencies = oBuilder
				.withLongName("use-overall-document-frequencies")
				.withDescription(
						"Calculate and use overall document frequencies")
				.create();

		Option addLowerNGrams = oBuilder.withLongName("add-lower-ngrams")
				.withDescription("Additionally adds the lower ngrams").create();

		Option skipAAAA = oBuilder.withLongName("skip-aaaa")
				.withDescription("Skips AAAA records").create();

		Group useNGramsChildren = gBuilder.withOption(addLowerNGrams)
				.withOption(skipAAAA).create();

		Option useNGrams = oBuilder
				.withLongName("use-ngrams")
				.withDescription(
						"Uses ngrams instead of single tokens. The supplied attributed is used for gramsize")
				.withArgument(aBuilder.withMaximum(1).withMinimum(1).create())
				.withChildren(useNGramsChildren).create();

		Option globalTopPatterns = oBuilder
				.withLongName("global-top-patterns")
				// .withArgument(aBuilder.withMaximum(1).create())
				.withDescription(
						"select the TopPatterns using all available Days.")
				.withRequired(false).create();

		Option perClassTopPatterns = oBuilder
				.withLongName("per-class-top-patterns")
				.withDescription(
						"select the TopPatterns on a per user level; the default is to select on a per instance level")
				.withRequired(false).create();
		
		Option candidatePatternPerUser = oBuilder
				.withLongName("num-top-patterns")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withDescription("sets the Number of Top-Patterns per User")
				.withRequired(false).create();

		Option skipFilterDataSet = oBuilder
				.withLongName("skip-filter-dataset")
				.withDescription(
						"skips filtering, assuming filtered dataset is already available")
				.withRequired(false).create();

		Group filterDataSetChildren = gBuilder
				.withOption(candidatePatternPerUser).withRequired(true)
				.withOption(globalTopPatterns).withRequired(false)
				.withOption(perClassTopPatterns).withRequired(false)
				.withOption(skipFilterDataSet).withRequired(false).create();

		Option cachingDuration = oBuilder.withLongName("caching-duration")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withDescription("select the Duration for Caching")
				.withRequired(false).create();

		Group cachingToolChildren = gBuilder
				.withOption(cachingDuration).withRequired(false).create();

		Option useCachingTool = oBuilder.withLongName("simulate-caching")
				.withDescription("filters the LogData to simulate Caching")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withRequired(false).withChildren(cachingToolChildren).create();

		Group createDataSetChildren = gBuilder
				.withOption(rawDataPathOption)
				// .withOption(hostIndexOption)
				.withOption(writeHostIndexMapping)
				.withOption(generateHeaderOnly).withOption(skipHeader)
				.withOption(skipNgrams)
				.withOption(skipDailyDocumentFrequencies)
				.withOption(minimumHostDFOption)
				.withOption(minimumHostOccurenceOption)
				.withOption(minimumRequestsPerUserOption)
				.withOption(maximumRequestsPerUserOption)
				.withOption(addSecondLvlDomainsOption)
				.withOption(addOnlyTypeAOption)
				.withOption(addIgnoreSophosxlOption)
				.withOption(addAllStaticOption)
				.withOption(lowercaseHostnamesOption)
				.withOption(rejectInvalidHostnamesOption)
				.withOption(rejectWindowsBrowserQueries)
				.withOption(useDynamicRequestRangeIndexOption)
				.withOption(dynamicRequestRangeSkipOnlyTopUsersOption)
				.withOption(dynamicRequestRangeSkipOnlyBottomUsersOption)
				.withOption(useRangeQuerySimulatorOption)
				.withOption(skipNMostPopularHostnamesOption)
				.withOption(onlyNMostPopularHostnamesOption)
				.withOption(ignoreFrequenciesOption)
				.withOption(dailyDocumentFrequencies)
				.withOption(overallDocumentFrequencies).withOption(useNGrams)
				.withOption(userWhiteListPath).withOption(hostWhiteListPath)
				.withOption(userBlackListPath).withOption(hostBlackListPath)
				.withOption(useCachingTool).create();

		Option createDataSetOption = oBuilder.withShortName("d")
				.withDescription("Generates the dataset")
				.withChildren(createDataSetChildren).create();

		numClassesOption = oBuilder
				.withLongName("num-classes")
				.withDescription(
						"Number of classes which will be randomly selected from the dataset")
				.withArgument(aBuilder.withMaximum(1).withMinimum(1).create())
				.withRequired(true).create();

		numInstancesPerClassOption = oBuilder
				.withLongName("num-instances-per-class")
				.withDescription(
						"Number of instances per class which a selected class must have")
				.withArgument(aBuilder.withMaximum(1).withMinimum(1).create())
				.withRequired(true).create();

		numTrainingInstancesPerClassOption = oBuilder
				.withLongName("num-training-instances-per-class")
				.withDescription(
						"Number of training instances for cross validation (default: all available, i.e. (num_folds-1)*(num_instances_per_class/num_folds)")
				.withArgument(aBuilder.withMaximum(1).withMinimum(1).create())
				.withRequired(false).create();

		seedOption = oBuilder
				.withLongName("seed")
				.withDescription(
						"Specifies the seed which is used for the random user selection. Defaults to 1")
				.withArgument(aBuilder.withMaximum(1).withMinimum(1).create())
				.create();

		Option classifier = oBuilder
				.withLongName("classifier")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withDescription(
						"select a Classifier, Default: 'mnb' multinominal naive bayes. Available: 'lift', 'support', 'mnb', 'cosim', 'jaccard'")
				.withRequired(false).create();

		Option interintra = oBuilder.withLongName("inter-intra")
				.withDescription(" ").withRequired(false).create();

		doCrossValidationChildren = gBuilder.withOption(numClassesOption)
				.withOption(numInstancesPerClassOption)
				.withOption(numTrainingInstancesPerClassOption)
				.withOption(seedOption).withOption(classifier)
				.withOption(candidatePatternPerUser)
				.withOption(globalTopPatterns).withOption(perClassTopPatterns).withOption(interintra).create();

		doCrossValidationOption = oBuilder
				.withShortName("cv")
				.withDescription(
						"Run cross-validation on data with specified number of folds")
				.withArgument(aBuilder.withMaximum(1).withMinimum(1).create())
				.withChildren(doCrossValidationChildren).create();

		Option testOnlyOption = oBuilder
				.withLongName("test-only")
				.withDescription(
						"Testing only, skips training. Useful for batch jobs")
				.create();

		Option ignoreSameTopPattern = oBuilder
				.withLongName("no-same-important-top-patterns")
				.withDescription(
						"No same 'important' Top-Patterns will be used.")
				.create();

		Option trainingOnlyOption = oBuilder
				.withLongName("training-only")
				.withDescription(
						"Training only, skips testing. Useful for batch jobs")
				.create();

		Option fromTimeOption = oBuilder
				.withLongName("from")
				.withDescription(
						"First session. Format: yyyy-MM-dd-HH-mm. If we are testing against a synthesized dataset the first session starts at 1970-01-01-00-00")
				.withRequired(true)
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option toTimeOption = oBuilder
				.withLongName("to")
				.withDescription(
						"Last session. Format: yyyy-MM-dd-HH-mm. If we are testing against a synthesized dataset the first session starts at 1970-01-01-00-00")
				.withRequired(true)
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option usePRCOption = oBuilder.withLongName("use-prc")
				.withDescription("Use probabilities of class").create();

		Option useRandomTrainingSessionsOption = oBuilder
				.withLongName("use-random-training-sessions")
				.withDescription(
						"Draw the training sessions randomly from the dataset. The supplied argument specifies how many days should be drawn")
				.withArgument(aBuilder.withMaximum(1).withMinimum(1).create())
				.create();

		Option offsetBetweenTrainAndTestOption = oBuilder
				.withLongName("offset-between-training-and-test")
				.withDescription(
						"Specifies the offset between the training and session. Default is 1440")
				.withArgument(aBuilder.withMaximum(1).withMinimum(1).create())
				.create();

		Option dropAmbiguousResults = oBuilder
				.withLongName("cosim-booster")
				.withDescription(
						"In case of ambiguous results, chooses the best instance based on cosine similarity")
				.create();
		
		Option simulateOmniscientClassifier = oBuilder
				.withLongName("simulate-omniscient-classifier")
				.withDescription(
						"Simulate an omniscient classifier whose predictions are always correct.")
				.create();

		Group trainTestChildren = gBuilder.withOption(testOnlyOption)
				.withOption(trainingOnlyOption)
				.withOption(candidatePatternPerUser).withOption(classifier)
				.withOption(globalTopPatterns).withOption(perClassTopPatterns).withOption(ignoreSameTopPattern)
				.withOption(fromTimeOption).withOption(toTimeOption)
				.withOption(usePRCOption)
				.withOption(useRandomTrainingSessionsOption)
				.withOption(offsetBetweenTrainAndTestOption).withRequired(true)
				.withOption(dropAmbiguousResults)
				.withOption(simulateOmniscientClassifier)
				.create();
			

		Option openWorldUserlistPathOption = oBuilder
				.withLongName("userlist-path")
				.withDescription(
						"Path to the user list of the user we are interested in")
				.withArgument(aBuilder.withMaximum(1).withMinimum(1).create())
				.withRequired(true).create();

		Group openWorldSimulationChidren = gBuilder
				.withOption(openWorldUserlistPathOption)
				.withOption(toTimeOption).withOption(fromTimeOption).create();

		doOpenWorldSimulationOption = oBuilder.withShortName("ow")
				.withChildren(openWorldSimulationChidren)
				.withDescription("Runs the open world simulation")
				.withArgument(aBuilder.withMaximum(2).withMinimum(2).create())
				.create();

		Option doTrainTestOption = oBuilder.withShortName("t")
				.withDescription("Do test and training")
				.withChildren(trainTestChildren).create();

		Option doAnalysisOption = oBuilder.withShortName("a")
				.withDescription("Do analysis").withChildren(trainTestChildren)
				.create();

		Option fromSynthesizeOption = oBuilder
				.withLongName("from")
				.withDescription(
						"First session from where the synthesized dataset should be drawn")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option toSynthesizeOption = oBuilder
				.withLongName("to")
				.withDescription(
						"Last session from where the synthesized dataset should be drawn")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Option numberOfSynthesizedDays = oBuilder
				.withLongName("number-of-synthesized-days")
				.withDescription("The number of synthesized days")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.create();

		Group synthesizeChildren = gBuilder.withOption(fromSynthesizeOption)
				.withOption(toSynthesizeOption)
				.withOption(numberOfSynthesizedDays).create();

		Option doSynthesize = oBuilder.withShortName("syn")
				.withDescription("Use synthesized dataset")
				.withChildren(synthesizeChildren).create();

		Option filterDataSet = oBuilder
				.withLongName("filter-dataset")
				.withDescription(
						"filters the DataSet to reduce it's size. use 'top-patterns-support' or 'top-patterns-lift'")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withRequired(false).withChildren(filterDataSetChildren)
				.withChildren(trainTestChildren).create();

		Option countEffectiveAttributesOption = oBuilder
				.withLongName("count-effective-attributes")
				.withArgument(aBuilder.withMinimum(1).withMaximum(1).create())
				.withDescription(
						"Determine the effective number of attributes and numQueriesPerInstance (header/effectiveNumOfAttributes): Use parameter to designate dataset to use: raw, tfn or all (to do both)")
				.withChildren(trainTestChildren).create();

		Group options = gBuilder.withOption(basePathOption)
				.withOption(sessionDurationOption)
				.withOption(sessionOffsetOption).withOption(numCoresOption)
				.withOption(numNodesOption).withOption(modeOption)
				.withOption(createDataSetOption).withOption(filterDataSet)
				.withOption(countEffectiveAttributesOption)
				.withOption(doCrossValidationOption)
				.withOption(doOpenWorldSimulationOption)
				.withOption(doTrainTestOption).withOption(doAnalysisOption)
				.withOption(doSynthesize).create();

		// configure a HelpFormatter
		HelpFormatter hf = new HelpFormatter();
		// hf.setFullUsageSettings(DisplaySetting.ALL);
		@SuppressWarnings("unchecked")
		Set<DisplaySetting> ds = hf.getDisplaySettings();
		ds.remove(DisplaySetting.DISPLAY_GROUP_NAME);

		// configure a parser
		Parser p = new Parser();
		p.setGroup(options);
		p.setHelpFormatter(hf);
		p.setHelpTrigger("--help");

		CommandLine cl = null;

		try {
			cl = p.parse(args);
		} catch (OptionException ex) {
			System.out.println(ex.getLocalizedMessage());
			hf.setGroup(options);
			hf.print();
		}

		// we're missing required arguments, so quit
		if (cl == null) {
			System.exit(-1);
		}

		String basePath = (String) cl.getValue(basePathOption);

		conf.set(Util.CONF_BASE_PATH, basePath);
		conf.set(Util.CONF_NGRAMS_PATH, basePath + "/ngrams");
		conf.set(Util.CONF_RANGE_QUERIES_PATH, basePath + "/rangequeries");
		conf.set(Util.CONF_HEADER_PATH, basePath + "/header");
		conf.set(Util.CONF_DATASET_PATH, basePath + "/dataset");
		conf.set(Util.CONF_DATASET_FILTER_PATH, basePath + "/dataset_filtered");
		conf.set(Util.CONF_CACHING_SIMULATOR_PATH, basePath + "/logfile_cached");
		conf.set(Util.CONF_TRAINING_PATH, basePath + "/training");
		conf.set(Util.CONF_DOCUMENT_FREQUENCY_PATH, basePath + "/df");
		conf.set(Util.CONF_TEST_PATH, basePath + "/test");
		conf.set(Util.CONF_MATRIX_PATH, basePath + "/matrix");
		conf.set(Util.CONF_ANALYSIS_PATH, basePath + "/analysis");
		conf.set(Util.CONF_CANDIDATEPATTERN_PATH, basePath
				+ "/candidatepattern");

		if (cl.hasOption(numCoresOption)) {
			conf.setInt(Util.CONF_NUM_CORES,
					Integer.parseInt((String) cl.getValue(numCoresOption)));
		}

		if (cl.hasOption(numNodesOption)) {
			conf.setInt(Util.CONF_NUM_NODES,
					Integer.parseInt((String) cl.getValue(numNodesOption)));
		}

		LOG.info("Optimizing for "
				+ conf.getInt(Util.CONF_NUM_NODES, Util.DEFAULT_NUM_NODES)
				+ " nodes with "
				+ conf.getInt(Util.CONF_NUM_CORES, Util.DEFAULT_NUM_CORES)
				+ " cores/node.");

		conf.setInt(Util.CONF_SESSION_DURATION,
				Integer.parseInt((String) cl.getValue(sessionDurationOption)));

		if (cl.hasOption(sessionOffsetOption)) {
			conf.setInt(Util.CONF_SESSION_OFFSET,
					Integer.parseInt((String) cl.getValue(sessionOffsetOption)));
		}

		if (cl.hasOption(candidatePatternPerUser)) {
			conf.setInt(Util.CONF_NUM_TOP_PATTERNS, Integer
					.parseInt((String) cl.getValue(candidatePatternPerUser)));
		}

		if (cl.hasOption(globalTopPatterns)) {
			// conf.setInt(Util.CONF_GLOBAL_TOP_PATTERNS,
			// Integer.parseInt((String) cl.getValue(globalTopPatterns)));
			conf.setBoolean(Util.CONF_GLOBAL_TOP_PATTERNS, true);
		}
		if (cl.hasOption(perClassTopPatterns)) {
			conf.setBoolean(Util.CONF_TOP_PATTERNS_PER_CLASS, true);
		}

		if (cl.hasOption(filterDataSet)) {
			conf.set(Util.CONF_DATASET_FILTER_METHOD,
					(String) cl.getValue(filterDataSet));
			conf.setBoolean(Util.CONF_DATASET_FILTER, true);
		}

		if (cl.hasOption(useCachingTool)) {
			conf.set(Util.CONF_CACHING_SIMULATOR,
					(String) cl.getValue(useCachingTool));
			if (!cl.hasOption(cachingDuration)) {
				conf.setInt(Util.CONF_CACHING_DURATION, Integer
						.parseInt((String) cl.getValue(sessionDurationOption)));
			}
		}

		if (cl.hasOption(cachingDuration)) {
			conf.setInt(Util.CONF_CACHING_DURATION,
					Integer.parseInt((String) cl.getValue(cachingDuration)));
		}

		if (cl.hasOption(ignoreSameTopPattern)) {
			conf.setBoolean(Util.CONF_NO_SAME_TOPPATTERNS, true);
		}

		if (cl.hasOption(classifier)) {
			conf.set(Util.CONF_CLASSIFIER, (String) cl.getValue(classifier));
		}
		// default classifier is MultinominalNaiveBayes
		else if (!cl.hasOption(classifier)) {
			conf.set(Util.CONF_CLASSIFIER, "mnb");
		}

		if (cl.hasOption(interintra)) {
			conf.setBoolean(Util.CONF_INTERINTRA, true);
		}
		conf.setInt(Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
				Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST);

		if (cl.hasOption(offsetBetweenTrainAndTestOption)) {
			conf.setInt(Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST, Integer
					.parseInt((String) cl
							.getValue(offsetBetweenTrainAndTestOption)));
		}

		if (cl.hasOption(useRandomTrainingSessionsOption)) {
			conf.setInt(Util.CONF_USE_RANDOM_TRAINING_SESSIONS, Integer
					.parseInt((String) cl
							.getValue(useRandomTrainingSessionsOption)));
		}

		conf.set(Util.CONF_OPTIONS, (String) cl.getValue(modeOption));

		conf.set(
				"mapred.child.java.opts",
				"-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:HeapDumpPath=/datastore/tmp -XX:+HeapDumpOnOutOfMemoryError");
		conf.set("hadoop.job.ugi", Util.HADOOP_USER);
		conf.set("mapred.task.timeout", "1800000");
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		if (cl.hasOption(minimumHostDFOption)) {
			conf.setFloat(Util.CONF_MINIMUM_DF_OF_HOSTS,
					Float.parseFloat((String) cl.getValue(minimumHostDFOption)));
		}

		/*
		 * for GenerateDatasetReducer: Do not add hosts to the histogram of a
		 * user profile which occur less than the given threshold in that
		 * profile. Set to 0 to add all hosts to profiles regardless of their
		 * occurrence frequency
		 */
		if (cl.hasOption(minimumHostOccurenceOption)) {
			conf.setInt(Util.CONF_MIN_HOST_OCCURRENCE_FREQUENCY, Integer
					.parseInt((String) cl.getValue(minimumHostOccurenceOption)));
		}

		/*
		 * for GenerateDatasetReducer: Do not add user profiles which contain
		 * less requests than the given threshold. Set to 0 to ignore this
		 * parameter
		 */
		if (cl.hasOption(minimumRequestsPerUserOption)) {
			conf.setInt(Util.CONF_MIN_REQUESTS_PER_PROFILE, Integer
					.parseInt((String) cl
							.getValue(minimumRequestsPerUserOption)));
		}

		/*
		 * for GenerateDatasetReducer: Do not add user profiles which contain
		 * more requests than the given threshold. Set to -1 to ignore this
		 * parameter
		 */
		if (cl.hasOption(maximumRequestsPerUserOption)) {
			conf.setInt(Util.CONF_MAX_REQUESTS_PER_PROFILE, Integer
					.parseInt((String) cl
							.getValue(maximumRequestsPerUserOption)));
		}

		/*
		 * for GenerateDatasetHeaderMapper + GenerateDataSetReducer: Determines
		 * whether 2nd level domains should be added to dataset
		 */
		if (cl.hasOption(addSecondLvlDomainsOption)) {
			conf.setBoolean(Util.CONF_ADD_SECOND_LEVEL_DOMAINS, true);
		}

		if (cl.hasOption(addIgnoreSophosxlOption)) {
			conf.setBoolean(Util.CONF_DATASET_IGNORE_SOPHOSXL, true);
		}

		if (cl.hasOption(addOnlyTypeAOption)) {
			conf.setBoolean(Util.CONF_DATASET_ONLY_TYPE_A_AND_AAAA, true);
		}

		if (cl.hasOption(addAllStaticOption)) {
			conf.setBoolean(Util.CONF_INCLUDE_ALL_STATIC_USERS, true);
		}

		conf.setInt(Util.CONF_SKIP_N_MOST_POPULAR_HOSTNAMES,
				Util.DEFAULT_SKIP_N_MOST_POPULAR_HOSTNAMES);
		if (cl.hasOption(skipNMostPopularHostnamesOption)) {
			conf.setInt(Util.CONF_SKIP_N_MOST_POPULAR_HOSTNAMES, Integer
					.parseInt((String) cl
							.getValue(skipNMostPopularHostnamesOption)));
		}

		conf.setInt(Util.CONF_ONLY_N_MOST_POPULAR_HOSTNAMES,
				Util.DEFAULT_ONLY_N_MOST_POPULAR_HOSTNAMES);
		if (cl.hasOption(onlyNMostPopularHostnamesOption)) {
			conf.setInt(Util.CONF_ONLY_N_MOST_POPULAR_HOSTNAMES, Integer
					.parseInt((String) cl
							.getValue(onlyNMostPopularHostnamesOption)));
		}

		conf.setBoolean(Util.CONF_IGNORE_FREQUENCIES,
				Util.DEFAULT_IGNORE_FREQUENCIES);
		if (cl.hasOption(ignoreFrequenciesOption)) {
			conf.setBoolean(Util.CONF_IGNORE_FREQUENCIES, true);
		}

		if (cl.hasOption(dailyDocumentFrequencies)) {
			conf.setBoolean(Util.CONF_USE_DAILY_DOCUMENT_FREQUENCIES, true);
		}

		if (cl.hasOption(overallDocumentFrequencies)) {
			conf.setBoolean(Util.CONF_USE_OVERALL_DOCUMENT_FREQUENCIES, true);
		}

		if (cl.hasOption(lowercaseHostnamesOption)) {
			conf.setBoolean(Util.CONF_DOWNCASE_HOSTNAMES, true);
		}

		if (cl.hasOption(rejectInvalidHostnamesOption)) {
			conf.setBoolean(Util.CONF_REJECT_INVALID_HOSTNAMES, true);
		}

		if (cl.hasOption(rejectWindowsBrowserQueries)) {
			conf.setBoolean(Util.CONF_REJECT_WINDOWS_BROWSER_QUERIES, true);
		}

		if (cl.hasOption(useDynamicRequestRangeIndexOption)) {
			conf.setBoolean(Util.CONF_USE_DYNAMIC_REQUEST_RANGE_INDEX, true);
		}

		if (cl.hasOption(dynamicRequestRangeSkipOnlyBottomUsersOption)) {
			conf.setBoolean(
					Util.CONF_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_BOTTOM_USERS,
					true);
		}

		if (cl.hasOption(dynamicRequestRangeSkipOnlyTopUsersOption)) {
			conf.setBoolean(
					Util.CONF_DYNAMIC_REQUEST_RANGE_SKIP_ONLY_TOP_USERS, true);
		}

		if (cl.hasOption(usePRCOption)) {
			conf.setBoolean(Util.CONF_USE_PRC, true);
		}

		Util.writeInfoFile(conf, fs, basePath, args);

		if (cl.hasOption(createDataSetOption)) {
			// set the path to the log data
			conf.set(Util.CONF_LOGDATA_PATH,
					(String) cl.getValue(rawDataPathOption));

			conf.setBoolean(
					Util.CONF_DATASET_USER_WHITELIST_ENABLED,
					(Boolean) Util
							.getDefault(Util.CONF_DATASET_USER_WHITELIST_ENABLED));
			if (cl.hasOption(userWhiteListPath)) {
				conf.set(Util.CONF_DATASET_USER_WHITELIST_PATH,
						(String) cl.getValue(userWhiteListPath));
				conf.setBoolean(Util.CONF_DATASET_USER_WHITELIST_ENABLED, true);
			}

			conf.setBoolean(
					Util.CONF_DATASET_HOST_WHITELIST_ENABLED,
					(Boolean) Util
							.getDefault(Util.CONF_DATASET_HOST_WHITELIST_ENABLED));
			if (cl.hasOption(hostWhiteListPath)) {
				conf.set(Util.CONF_DATASET_HOST_WHITELIST_PATH,
						(String) cl.getValue(hostWhiteListPath));
				conf.setBoolean(Util.CONF_DATASET_HOST_WHITELIST_ENABLED, true);
			}

			conf.setBoolean(
					Util.CONF_DATASET_USER_BLACKLIST_ENABLED,
					(Boolean) Util
							.getDefault(Util.CONF_DATASET_USER_BLACKLIST_ENABLED));
			if (cl.hasOption(userBlackListPath)) {
				conf.set(Util.CONF_DATASET_USER_BLACKLIST_PATH,
						(String) cl.getValue(userBlackListPath));
				conf.setBoolean(Util.CONF_DATASET_USER_BLACKLIST_ENABLED, true);
			}

			conf.setBoolean(
					Util.CONF_DATASET_HOST_BLACKLIST_ENABLED,
					(Boolean) Util
							.getDefault(Util.CONF_DATASET_HOST_BLACKLIST_ENABLED));
			if (cl.hasOption(hostBlackListPath)) {
				conf.set(Util.CONF_DATASET_HOST_BLACKLIST_PATH,
						(String) cl.getValue(hostBlackListPath));
				conf.setBoolean(Util.CONF_DATASET_HOST_BLACKLIST_ENABLED, true);
			}

			if (cl.hasOption(writeHostIndexMapping)) {
				conf.setBoolean(Util.CONF_WRITE_HOST_INDEX_MAPPING, true);
			}

			if (cl.hasOption(useCachingTool)) {
				// run the CachingTool
				if (ToolRunner.run(conf, new CachingTool(), null) == 0) {
					return 0;
				}
				conf.set(
						Util.CONF_LOGDATA_PATH,
						conf.get(Util.CONF_CACHING_SIMULATOR_PATH) + "/"
								+ conf.get(Util.CONF_SESSION_DURATION));
			}
			// range queries simulator
			if (cl.hasOption(useRangeQuerySimulatorOption)) {
				/*
				 * number of dummy samples; number of hosts to choose from the
				 * dummy pool. Typically: 100000
				 */
				if (cl.hasOption(numberOfDummySamplesOption)) {
					conf.setInt(
							Util.CONF_RANGE_QUERIES_NUMBER_OF_DUMMY_SAMPLES,
							Integer.parseInt((String) cl
									.getValue(numberOfDummySamplesOption)));
				}

				/*
				 * Type of range queries; set to true to always generate the
				 * same dummies for a given Host. set to false to generate
				 * totally random dummies for each occurrence of a given host.
				 */
				if (cl.hasOption(generateStaticDummiesOption)) {
					conf.setBoolean(Util.CONF_GENERATE_STATIC_DUMMIES, Boolean
							.parseBoolean((String) cl
									.getValue(generateStaticDummiesOption)));
				}

				/*
				 * Number of dummy requests to insert per real request Set to 0
				 * to not generate dummies.
				 */
				if (cl.hasOption(numberOfDummiesPerRequestOption)) {
					conf.setInt(Util.CONF_NUMBER_OF_DUMMIES_PER_REQUEST,
							Integer.parseInt((String) cl
									.getValue(numberOfDummiesPerRequestOption)));
				}

				if (cl.hasOption(dummyHostPathOption)) {
					conf.set(Util.CONF_DUMMY_HOST_PATH,
							(String) cl.getValue(dummyHostPathOption));
				}

				// run range queries simulator
				if (ToolRunner.run(conf, new RangeQueriesSimulatorTool(), null) == 0) {
					return 0;
				}

				// after the range queries script has been succesfully run, we
				// change the logdata path to our range-queries-directory. this
				// way
				// we don't need to change anything in the
				// GenerateDataSetHeaderTool and GenerateDataSetTool
				conf.set(
						Util.CONF_LOGDATA_PATH,
						conf.get(Util.CONF_RANGE_QUERIES_PATH) + "/"
								+ conf.get(Util.CONF_SESSION_DURATION));

			}

			conf.setInt(Util.CONF_NGRAMS_SIZE, Util.DEFAULT_NGRAMS_SIZE);
			// if we are using ngrams, set the correct size and run the generate
			// ngrams script
			if (cl.hasOption(useNGrams)) {
				int gramsize = Integer
						.parseInt((String) cl.getValue(useNGrams));

				// no need to run the script if a gramsize of 1 is specified
				// maybe run it for debug reasons anyway?
				if (gramsize > 1) {
					conf.setInt(Util.CONF_NGRAMS_SIZE, gramsize);
					conf.setBoolean(Util.CONF_NGRAMS_ADD_LOWER,
							Util.DEFAULT_NGRAMS_PRESERVE_1GRAMS);
					if (cl.hasOption(addLowerNGrams)) {
						conf.setBoolean(Util.CONF_NGRAMS_ADD_LOWER, true);
					}

					conf.setBoolean(Util.CONF_NGRAMS_SKIP_AAAA,
							Util.DEFAULT_NGRAMS_SKIP_AAAA);
					if (cl.hasOption(skipAAAA)) {
						conf.setBoolean(Util.CONF_NGRAMS_SKIP_AAAA, true);
					}

					// turn off reject invalid hostnames and add2ndleveldomain
					// they're not compatible with ngrams > 1
					conf.setBoolean(Util.CONF_ADD_SECOND_LEVEL_DOMAINS, false);
					conf.setBoolean(Util.CONF_REJECT_INVALID_HOSTNAMES, false);

					if (!cl.hasOption(skipNgrams)) {
						if (ToolRunner
								.run(conf, new GenerateNGramsTool(), null) == 0) {
							return 0;
						}
					}

					// after the ngram script has been succesfully run, we
					// change the logdata path to our ngrams-directory. this way
					// we don't need to change anything in the
					// GenerateDataSetHeaderTool and GenerateDataSetTool
					conf.set(
							Util.CONF_LOGDATA_PATH,
							conf.get(Util.CONF_NGRAMS_PATH) + "/"
									+ conf.get(Util.CONF_SESSION_DURATION));
				}
			}

			if (!cl.hasOption(skipHeader)) {
				// run the GenerateDataSetHeaderTool
				if (ToolRunner.run(conf, new GenerateDataSetHeaderTool(), null) == 0) {
					return 0;
				}
				
				// copy counters into header
				if (cl.hasOption(useCachingTool)) {
					Path src = (new Path(conf.get(Util.CONF_CACHING_SIMULATOR_PATH))).
							suffix("/"+DataSetHeader.SIMULATE_CACHING_METADATA_FILE);
					Path dst    = (new Path(conf.get(Util.CONF_HEADER_PATH))).
							suffix("/"+DataSetHeader.SIMULATE_CACHING_METADATA_FILE);
					FileUtil.copy(fs, src, fs, dst, false, true, conf); 
				}
			}
			



			if (cl.hasOption(dailyDocumentFrequencies)) {
				if (!cl.hasOption(skipDailyDocumentFrequencies)) {
					if (ToolRunner.run(conf, new DailyDocumentFrequencyTool(),
							null) == 0) {
						return 0;
					}
				}
			}

			if (cl.hasOption(overallDocumentFrequencies)) {
				if (ToolRunner.run(conf, new OverallDocumentFrequencyTool(),
						null) == 0) {
					return 0;
				}
			}

			if (!cl.hasOption(generateHeaderOnly)) {
				// run the GenerateDataSetTool
				if (ToolRunner.run(conf, new GenerateDataSetTool(), null) == 0) {
					return 0;
				}
			}
		}

		// Filtering for CrossValidation is launched in the
		// CrossValidationTool
		if (cl.hasOption(filterDataSet)
				&& !(cl.hasOption(doCrossValidationOption))) {
			conf.setIfUnset(Util.CONF_FIRST_SESSION,
					(String) cl.getValue(fromTimeOption));
			conf.setIfUnset(Util.CONF_LAST_SESSION,
					(String) cl.getValue(toTimeOption));

			if (!cl.hasOption(skipFilterDataSet)) {
				// first run the CandidatePatternTool
				if (ToolRunner.run(conf, new CandidatePatternTool(), null) == 0) {
					throw new Exception("CandidatePattern creation failed");
				}

				// run the FilterDataSetTool
				if (ToolRunner.run(conf, new FilterDataSetTool(), null) == 0) {
					return 0;
				}
			}
			// set the newly created and filtered DataSet as the DataSet to use
			conf.set(Util.CONF_DATASET_PATH,
					conf.get(Util.CONF_DATASET_FILTER_PATH));
		}

		if (cl.hasOption(countEffectiveAttributesOption)) {
			// run CountEffectiveNumOfAttributesTool
			conf.setIfUnset(Util.CONF_FIRST_SESSION,
					(String) cl.getValue(fromTimeOption));
			conf.setIfUnset(Util.CONF_LAST_SESSION,
					(String) cl.getValue(toTimeOption));
			conf.set(Util.CONF_COUNT_EFFECTIVE_ATTRIBUTES_OPTION,
					(String) cl.getValue(countEffectiveAttributesOption));
			
			if(conf.get(Util.CONF_COUNT_EFFECTIVE_ATTRIBUTES_OPTION).equals("all")) {
				
				conf.set(Util.CONF_COUNT_EFFECTIVE_ATTRIBUTES_OPTION,
						"raw");
				if (ToolRunner.run(conf, new CountEffectiveNumOfAttributesTool(),
						null) == 0) {
					return 0;
				}
				conf.set(Util.CONF_COUNT_EFFECTIVE_ATTRIBUTES_OPTION,
						"tfn");
				if (ToolRunner.run(conf, new CountEffectiveNumOfAttributesTool(),
						null) == 0) {
					return 0;
				}
				
			} else {
				if (ToolRunner.run(conf, new CountEffectiveNumOfAttributesTool(),
						null) == 0) {
					return 0;
				}
			}
		}

		if (cl.hasOption(doSynthesize)) {
			conf.setBoolean(Util.CONF_USE_SYNTHESIZE, true);
			conf.set(Util.CONF_FIRST_SYNTHESIZED_SESSION,
					(String) cl.getValue(fromSynthesizeOption));
			conf.set(Util.CONF_FIRST_SYNTHESIZED_SESSION,
					(String) cl.getValue(toSynthesizeOption));

			// run the SynthesizeDataSetTool
			if (ToolRunner.run(conf, new SynthesizeDataSetTool(), null) == 0) {
				return 0;
			}
		}

		if (cl.hasOption(doOpenWorldSimulationOption)) {
			/*
			 * conf.set(Util.CONF_FIRST_SESSION, (String)
			 * cl.getValue(fromTimeOption)); conf.set(Util.CONF_LAST_SESSION,
			 * (String) cl.getValue(toTimeOption));
			 * 
			 * List values = cl.getValues(doOpenWorldSimulationOption);
			 * 
			 * conf.setInt(Util.CONF_OPEN_WORLD_NUM_TRAINING_INSTANCES,
			 * Integer.parseInt((String) values.get(0)));
			 * conf.setInt(Util.CONF_OPEN_WORLD_NUM_TEST_INSTANCES,
			 * Integer.parseInt((String) values.get(1)));
			 * 
			 * conf.set(Util.CONF_OPEN_WORLD_USERLIST_PATH, (String)
			 * cl.getValue(openWorldUserlistPathOption));
			 * 
			 * if (ToolRunner.run(conf, new OpenWorldSimulationTool(), null) ==
			 * 0) { return 1; }
			 * 
			 * conf.set(Util.CONF_DATASET_PATH, basePath + "/openworldDataset");
			 */
		}

		if (cl.hasOption(doCrossValidationOption)) {
			conf.setInt(Util.CONF_CROSS_VALIDATION_NUM_FOLDS, Integer
					.parseInt((String) cl.getValue(doCrossValidationOption)));
			conf.setInt(Util.CONF_CROSS_VALIDATION_NUM_OF_CLASSES,
					Integer.parseInt((String) cl.getValue(numClassesOption)));
			conf.setInt(Util.CONF_CROSS_VALIDATION_NUM_INSTANCES_PER_CLASS,
					Integer.parseInt((String) cl
							.getValue(numInstancesPerClassOption)));

			if (cl.hasOption(numTrainingInstancesPerClassOption)) {
				conf.setInt(
						Util.CONF_CROSS_VALIDATION_NUM_TRAINING_INSTANCES_PER_CLASS,
						Integer.parseInt((String) cl
								.getValue(numTrainingInstancesPerClassOption)));
			}

			conf.setInt(Util.CONF_CROSS_VALIDATION_SEED,
					(Integer) Util.getDefault(Util.CONF_CROSS_VALIDATION_SEED));

			if (cl.hasOption(seedOption)) {
				conf.set(Util.CONF_CROSS_VALIDATION_SEED,
						(String) cl.getValue(seedOption));
			}

			if (ToolRunner.run(conf, new CrossValidationTool(), null) == 0) {
				return 1;
			}

			return 0;
		}

		if (cl.hasOption(doTrainTestOption)) {

			if (cl.hasOption(dropAmbiguousResults)) {
				conf.setBoolean(Util.CONF_TEST_DROP_AMBIGUOUS_RESULTS, true);
			}

			conf.set(Util.CONF_FIRST_SESSION,
					(String) cl.getValue(fromTimeOption));
			conf.set(Util.CONF_LAST_SESSION, (String) cl.getValue(toTimeOption));

			Calendar firstSession = Util.getCalendarFromString(conf
					.get(Util.CONF_FIRST_SESSION));
			Calendar lastSession = Util.getCalendarFromString(conf
					.get(Util.CONF_LAST_SESSION));

			if ((conf.get(Util.CONF_CLASSIFIER)).equals("lift")
					|| (conf.get(Util.CONF_CLASSIFIER)).equals("support")) {
				if (ToolRunner.run(conf, new CandidatePatternTool(), null) == 0) {
					throw new Exception("CandidatePattern creation failed");
				}
			}

			if (!cl.hasOption(testOnlyOption)) {
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
			}

			if (!cl.hasOption(trainingOnlyOption)) {
				if (!cl.hasOption(useRandomTrainingSessionsOption)) {
					Calendar trainingSession = (Calendar) firstSession.clone();
					Calendar testSession;

					while (trainingSession.before(lastSession)) {
						// run the test and confusion matrix for the
						// specified training and test session
						testSession = (Calendar) trainingSession.clone();
						testSession.add(Calendar.MINUTE, conf.getInt(
								Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
								Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST));

						if (Util.isDataAvailableForTrainingAndTest(conf,
								trainingSession, testSession)) {

							Util.showStatus(Util.formatDate(trainingSession
									.getTime())
									+ " -> "
									+ Util.formatDate(testSession.getTime()));

							conf.set(Util.CONF_TRAINING_DATE,
									Util.formatDate(trainingSession.getTime()));
							conf.set(Util.CONF_TEST_DATE,
									Util.formatDate(testSession.getTime()));

							if ((conf.get(Util.CONF_CLASSIFIER)).equals("lift")
									|| (conf.get(Util.CONF_CLASSIFIER))
											.equals("support")) {
								if (ToolRunner.run(conf, new YangTestTool(),
										null) == 0) {
									break;
								}
							}

							else if ((conf.get(Util.CONF_CLASSIFIER))
									.equals("mnb")) {
								if (ToolRunner.run(conf, new TestTool(), null) == 0) {
									break;
								}
							}

							else if ((conf.get(Util.CONF_CLASSIFIER))
									.equals("jaccard")) {
								if (ToolRunner.run(conf, new JaccardTestTool(),
										null) == 0) {
									break;
								}
							}

							else if ((conf.get(Util.CONF_CLASSIFIER))
									.equals("cosim")) {
								if (ToolRunner.run(conf, new CosimTestTool(),
										null) == 0) {
									break;
								}
							}

							if (ToolRunner.run(conf, new ConfusionMatrixTool(),
									null) == 0) {
								break;
							}
						}
						// jump to the next training session
						trainingSession.add(Calendar.MINUTE, conf.getInt(
								Util.CONF_SESSION_DURATION,
								Util.DEFAULT_SESSION_DURATION));
					}
				} else {

					// ////// DIESER CODE GEHT DERZEIT NICHT!!!!!!!!!!!
					/*
					 * if (true) throw new Exception(
					 * "Dieser Code wurde nicht mehr " +
					 * "aktualisiert und ist derzeit nicht lauff�hig");
					 * 
					 * int days = conf.getInt(
					 * Util.CONF_USE_RANDOM_TRAINING_SESSIONS, 0);
					 * 
					 * Calendar trainingSession, testSession;
					 * 
					 * Random seedGenerator = new Random(); int seed =
					 * seedGenerator.nextInt();
					 * 
					 * Random generator = new Random(seed);
					 * 
					 * conf.setInt(Util.CONF_RANDOM_TRAINING_SESSIONS_SEED,
					 * seed);
					 * 
					 * LOG.info("Setting seed: " + seed);
					 * 
					 * int sessionDuration = conf.getInt(
					 * Util.CONF_SESSION_DURATION,
					 * Util.DEFAULT_SESSION_DURATION); int sessionStartOffset =
					 * conf.getInt( Util.CONF_SESSION_OFFSET,
					 * Util.DEFAULT_SESSION_OFFSET); // TODO Offset müsste hier
					 * noch eingearbeitet werden
					 * 
					 * int sessionOffset = conf.getInt(
					 * Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
					 * Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST); int max =
					 * (int) ((lastSession.getTimeInMillis() - firstSession
					 * .getTimeInMillis()) / (1000 * 60)) - sessionOffset;
					 * 
					 * String drawnSessionsPath = basePath +
					 * "/training/drawn-random-sessions-" + days + "-" +
					 * sessionOffset;
					 * 
					 * fs.delete(new Path(drawnSessionsPath), false);
					 * 
					 * FSDataOutputStream fos = fs.create(new Path(
					 * drawnSessionsPath));
					 * 
					 * fos.writeBytes("Setting seed: " + seed + "\n");
					 * 
					 * for (int i = 0; i < days; i++) { do { int timeLapsed =
					 * (generator.nextInt(max) / sessionDuration)
					 * sessionDuration; trainingSession = (Calendar)
					 * firstSession.clone();
					 * trainingSession.add(Calendar.MINUTE, timeLapsed); } while
					 * (usedDates.contains(Util
					 * .formatDate(trainingSession.getTime())));
					 * 
					 * testSession = (Calendar) trainingSession.clone();
					 * testSession.add(Calendar.MINUTE, sessionOffset);
					 * 
					 * conf.set(Util.CONF_TRAINING_DATE,
					 * Util.formatDate(trainingSession.getTime()));
					 * conf.set(Util.CONF_TEST_DATE,
					 * Util.formatDate(testSession.getTime()));
					 * 
					 * // TODO: sollte dieser Code wieder lauffähig werden //
					 * muss // hier eine // Fallunterscheidung der verschiedenen
					 * TestTools // erfolgen if (ToolRunner.run(conf, new
					 * TestTool(), null) == 0) { break; }
					 * 
					 * if (ToolRunner.run(conf, new ConfusionMatrixTool(), null)
					 * == 0) { break; }
					 * 
					 * // OccurenceData is statically accessible through the //
					 * OccurenceMatrix OccurenceMatrix occTraining =
					 * OccurenceMatrix .getOccurenceData(null, conf, Util
					 * .formatDate(trainingSession.getTime())); OccurenceMatrix
					 * occTest = OccurenceMatrix .getOccurenceData(null, conf,
					 * Util.formatDate(testSession.getTime()));
					 * 
					 * int stillThere = 0; // loop through all classes of the
					 * training session and // check if they still occur at the
					 * test session for (String classLabel :
					 * occTraining.keySet()) { if
					 * (occTest.containsKey(classLabel)) { stillThere++; } }
					 * 
					 * double percentageOfActiveUsers = (double) stillThere /
					 * occTraining.size();
					 * fos.writeBytes(Util.formatDate(trainingSession
					 * .getTime()) + " -> " +
					 * Util.formatDate(testSession.getTime()) + "\t" +
					 * percentageOfActiveUsers + "\n");
					 * 
					 * usedDates
					 * .add(Util.formatDate(trainingSession.getTime()));
					 * fos.flush();
					 * 
					 * occTraining = null; occTest = null; }
					 * 
					 * fos.close();
					 */

					// ////// DIESER CODE GEHT DERZEIT NICHT!!!!!!!!!!!
				}
			}
		}

		if (cl.hasOption(doAnalysisOption)) {
			conf.set(Util.CONF_FIRST_SESSION,
					(String) cl.getValue(fromTimeOption));
			conf.set(Util.CONF_LAST_SESSION, (String) cl.getValue(toTimeOption));

			Calendar firstSession = Util.getCalendarFromString(conf
					.get(Util.CONF_FIRST_SESSION));
			Calendar lastSession = Util.getCalendarFromString(conf
					.get(Util.CONF_LAST_SESSION));

			if (cl.hasOption(simulateOmniscientClassifier)) {
				conf.setBoolean(Util.CONF_OMNISCIENT_CLASSIFIER, true);
			}
			
			String jobName = null;
			if ((conf.get(Util.CONF_CLASSIFIER)).equals("lift")
					|| (conf.get(Util.CONF_CLASSIFIER)).equals("support")) {
				jobName = Util.JOB_NAME_YANG + " [" + DNSAnalysisTool2.ACTION
						+ "] {test=" + conf.get(Util.CONF_TEST_PATH)
						+ ", session=" + conf.get(Util.CONF_SESSION_DURATION)
						+ "}";
			}

			else if ((conf.get(Util.CONF_CLASSIFIER)).equals("mnb")) {
				jobName = Util.JOB_NAME + " [" + DNSAnalysisTool2.ACTION
						+ "] {test=" + conf.get(Util.CONF_TEST_PATH)
						+ ", session=" + conf.get(Util.CONF_SESSION_DURATION)
						+ "}";
			}

			else if ((conf.get(Util.CONF_CLASSIFIER)).equals("cosim")) {
				jobName = Util.JOB_NAME_COSIM + " [" + DNSAnalysisTool2.ACTION
						+ "] {test=" + conf.get(Util.CONF_TEST_PATH)
						+ ", session=" + conf.get(Util.CONF_SESSION_DURATION)
						+ "}";
			}

			else if ((conf.get(Util.CONF_CLASSIFIER)).equals("jaccard")) {
				jobName = Util.JOB_NAME_JACCARD + " ["
						+ DNSAnalysisTool2.ACTION + "] {test="
						+ conf.get(Util.CONF_TEST_PATH) + ", session="
						+ conf.get(Util.CONF_SESSION_DURATION) + "}";
			}

			String fromTo = Util.formatDate(firstSession.getTime()) + " to "
					+ Util.formatDate(lastSession.getTime());

			if (cl.hasOption(useRandomTrainingSessionsOption)) {
				int days = conf.getInt(Util.CONF_USE_RANDOM_TRAINING_SESSIONS,
						0);
				int offset = conf.getInt(
						Util.CONF_OFFSET_BETWEEN_TRAINING_AND_TEST,
						Util.DEFAULT_OFFSET_BETWEEN_TRAINING_AND_TEST);
				fromTo = days + " random sessions from " + fromTo + " with "
						+ offset + " offset";
			}

			Path outputPath = new Path(conf.get(Util.CONF_ANALYSIS_PATH) + "/"
					+ conf.get(Util.CONF_SESSION_DURATION) + "/"
					+ conf.get(Util.CONF_OPTIONS) + "/" + fromTo);

			if (!cl.hasOption(useRandomTrainingSessionsOption)) {
				// makes sure that last confusion matrix is already there
				Path lastConfusionMatrixPath = new Path(
						conf.get(Util.CONF_MATRIX_PATH) + "/"
								+ conf.get(Util.CONF_SESSION_DURATION) + "/"
								+ conf.get(Util.CONF_OPTIONS) + "/"
								+ Util.formatDate(lastSession.getTime()) + "/"
								+ "part-r-00000");

				int i = 0;
				while (!fs.exists(lastConfusionMatrixPath) && (i++ < 120)) {
					LOG.info("Waiting for last confusion matrix... (max 120sec)");
					Thread.sleep(1 * 1000);
				}

				if (!fs.exists(lastConfusionMatrixPath)) {
					throw new Exception(
							"Waited 120 seconds, but last confusion matrix is still missing!");
				}
			} else {
				LOG.info("To be safe... wait 30sec...");
				Thread.sleep(30 * 1000);
			}

			Job job = new Job(conf, jobName);

			// set the number of reducers
			job.setNumReduceTasks(NUM_OF_REDUCE_TASKS);

			// set mapper class. we don't specifiy a reducer class so hadoop
			// uses the default one
			job.setJarByClass(TestTool.class);
			job.setMapperClass(DNSAnalysisMapper.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job,
					new Path(conf.get(Util.CONF_HEADER_PATH) + "/"
							+ DataSetHeader.USER_INDEX_DIRECTORY));

			// clear output directory
			fs.delete(outputPath, true);

			FileOutputFormat.setOutputPath(job, outputPath);

			boolean b = job.waitForCompletion(true);

			// write the populated <i>SymbolCounter<i> to a summary file
			Counters counters = job.getCounters();
			FSDataOutputStream fos = fs.create(outputPath.suffix("/counters"));
			fos.writeBytes("symbol\t\tcounter\n");

			for (DNSAnalysisMapper.SymbolCounter c : DNSAnalysisMapper.SymbolCounter
					.values()) {
				if (c != DNSAnalysisMapper.SymbolCounter.ALL) {
					fos.writeBytes(c.getSymbol() + "\t\t"
							+ counters.findCounter(c).getValue() + "\n");
				}
			}

			// gather info about how many plus'es and pipe's we've got. those 2
			// symbols
			// are considered 'correct'.
			long plus = counters.findCounter(
					DNSAnalysisMapper.SymbolCounter.PLUS).getValue();
			long pipe = counters.findCounter(
					DNSAnalysisMapper.SymbolCounter.PIPE).getValue();
			long all = counters
					.findCounter(DNSAnalysisMapper.SymbolCounter.ALL)
					.getValue();

			fos.writeBytes("=======================\n");
			fos.writeBytes("sum\t\t" + all + "\n");

			// write out the percentage of 'correct' symbols
			fos.writeBytes("%\t\t"
					+ NumberFormat.getInstance(Locale.ENGLISH).format(
							(double) (plus + pipe) / all) + "\n");

			fos.close();
			
			
			if (!cl.hasOption(useRandomTrainingSessionsOption)) {
				if (ToolRunner
						.run(conf, new AveragePrecisionRecallTool(), null) == 0) {
					return 0;
				}
			}

			return b ? 1 : 0;
		}

		// For False Positive Analysis (not working ATM)
		/*
		 * conf.setBoolean(Util.CONF_TRAINING_WITH_LIMITED_DATASET, true);
		 * conf.setInt(Util.CONF_TRAINING_USER_RATIO, 10);
		 */

		System.exit(0);

		return 1;
	}
}