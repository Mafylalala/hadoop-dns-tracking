package de.sec.dns.openworld;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import de.sec.dns.dataset.Instance;
import de.sec.dns.util.Util;

/**
 * The reducer class for {@link OpenWorldSimulationTool}. It creates multiple splits of the dataset
 * for cross validation
 * 
 * @author Christian Banse, Dominik Herrmann
 */
public class OpenWorldSimulationReducer extends Reducer<Text, Text, NullWritable, Text>
{

/**
 * A logger.
 */
private static final Log LOG = LogFactory.getLog(OpenWorldSimulationReducer.class);

/**
 * Used to handle multiple output files.
 */
private MultipleOutputs<NullWritable, Text> m;

/**
 * Store a record of already used classLabels per day for openWorldProblem; necessary to avoid the
 * (rather unlikely) case that 2 instances of the same class have been mapped to the same date. In
 * this case only the first instance should be used.
 */
private HashMap<String, Integer> openWorldUsedClasses = new HashMap<String, Integer>();

/**
 * how many world instances should be assigned to each date
 */
private int NUMBER_OF_WORLD_INSTANCES;

@Override
public void cleanup(Context context)
{
    try
    {
        // close the multiple output handler
        m.close();
    } catch (Exception e)
    {
        e.printStackTrace();
    }
}

/**
 * values are instances (in text representation)
 */
@Override
protected void reduce(Text something, Iterable<Text> values, Context context) throws IOException,
        InterruptedException
{
    String date;
    Configuration conf = context.getConfiguration();

    if (something.toString().startsWith("INTERESTING_CLASS"))
    {
        String[] rr = Util.veryFastSplit(something.toString(), '\t', 3);
        // rr = { INTERESTING_CLASS, class, date }
        date = rr[2];

        Text instance = values.iterator().next();

        m.write(NullWritable.get(), new Text(instance.toString()), conf.get(Util.CONF_SESSION_DURATION) + "/"
                + conf.get(Util.CONF_OPTIONS) + "/" + date + "/");
    }
    else
    {
        String[] rr = Util.veryFastSplit(something.toString(), '\t', 2);
        // rr = { UNINTERESTING_CLASS, date }
        date = rr[1];

        openWorldUsedClasses.clear();

        try
        {
            for (Text instance : values)
            {
                String classLabel = Instance.fromString(null, instance.toString(), context).getClassLabel();

                if (openWorldUsedClasses.containsKey(classLabel))
                    continue;

                openWorldUsedClasses.put(classLabel, 1);

                m.write(NullWritable.get(), new Text(instance.toString()),
                        conf.get(Util.CONF_SESSION_DURATION) + "/" + conf.get(Util.CONF_OPTIONS) + "/" + date
                                + "/");

            }
        } catch (Exception e)
        {
            throw new IOException("Parse error during reading instance");
        }
    }
}

@Override
public void setup(Context context)
{
    try
    {
        Configuration conf = context.getConfiguration();

        NUMBER_OF_WORLD_INSTANCES = Util.getInt(conf, Util.CONF_OPEN_WORLD_NUM_TRAINING_INSTANCES);

        // initialize the multiple outputs handler
        m = new MultipleOutputs<NullWritable, Text>(context);
    } catch (Exception e)
    {
        e.printStackTrace();
    }
}

}
