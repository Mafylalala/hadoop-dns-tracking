package de.sec.dns.dataset;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import de.sec.dns.test.YangTestReducer;
import de.sec.dns.training.CandidatePatternTool;
import de.sec.dns.util.Util;

/**
 * The training mapper for {@link CandidatePatternTool}. It hands an tuple
 * consisting of the attribute index and the occurrence of the attribute to the
 * reducer.
 * 
 * @author Elmo Randschau
 */
public class FilterDataSetMapper extends
		Mapper<LongWritable, ObjectWritable, Text, Text> {
	/**
	 * a Map containing the Candidate-Patterns of a date
	 */
	private HashMap<String, TIntArrayList> _candidateMap;

	private Text k = new Text();
	private Text v = new Text();

	private DataSetHeader header;

	private static final Log LOG = LogFactory.getLog(FilterDataSetMapper.class);

	@Override
	public void map(LongWritable key, ObjectWritable obj, Context context)
			throws IOException, InterruptedException {
		Instance instance;
		String classLabel;

		FileSplit file = (FileSplit) context.getInputSplit();

		// Extract date from path
		String date = file.getPath().getParent().getName();

		// Extract the option from the DataSet path
		String option_path = file.getPath().getParent().getParent().getName();

		instance = (Instance) obj.get();
		// classLabel = instance.getClassLabel();
		int numAttributes = 0;
		String id = instance.getId();

		TIntArrayList candidatePatterns = _candidateMap.get(date);

		StringBuilder sb = new StringBuilder();

		for (int a = 0; a < instance.getNumValues(); a++) {
			if (candidatePatterns.contains(instance.getIndex(a))) {
				numAttributes++;
				sb.append(instance.getIndex(a)).append(" ");
				sb.append(instance.getValue(a)).append(" ");
			}
		}
		sb.insert(0, numAttributes + " { ");
		sb.append("} " + id);
		k.set(date + " " + option_path);
		v.set(sb.toString());
		context.write(k, v);
	}

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();

		try {
			// TODO: readCandidatePattern so anpassen das immer nur der aktuelle
			// Tag geladen wird
			Path headerPath = new Path(conf.get(Util.CONF_HEADER_PATH));
			_candidateMap = Util.readCandidatePattern(context);
			header = new DataSetHeader(conf, headerPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
