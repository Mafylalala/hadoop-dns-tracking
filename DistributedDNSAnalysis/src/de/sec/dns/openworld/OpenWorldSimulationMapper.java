package de.sec.dns.openworld;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import de.sec.dns.dataset.Instance;
import de.sec.dns.util.ClassIndex;
import de.sec.dns.util.Util;

public class OpenWorldSimulationMapper extends Mapper<LongWritable, ObjectWritable, Text, Text>
{

static int instances = 0;

/**
 * A logger.
 */
private static final Log LOG = LogFactory.getLog(OpenWorldSimulationMapper.class);

/**
 * A temporary variable used as mapreduce key.
 */
private Text k = new Text();

/**
 * is set by setup method in case of fatal failure
 */
private Exception setupFailedException;

/**
 * A temporary variable used as mapreduce value.
 */
private Text v = new Text();

private HashMap<String, Boolean> interestingUsers = new HashMap<String, Boolean>();

private String[] trainingClasses;
private String[] testClasses;

/*
 * { //interestingUsers.put("d68_2", true); // zum lokalen Testen
 * //interestingUsers.put("3e2_1", true);
 * 
 * interestingUsers.put("ee_1", true); interestingUsers.put("ef_1", true);
 * interestingUsers.put("f10_1", true); interestingUsers.put("f23_1", true);
 * interestingUsers.put("f6d_1", true); interestingUsers.put("f9_1", true);
 * interestingUsers.put("fae_1", true); interestingUsers.put("fb_1", true);
 * interestingUsers.put("fc0_1", true); interestingUsers.put("fc4_1", true);
 * interestingUsers.put("118b_1", true); interestingUsers.put("11e_1",
 * true); }
 */

Calendar firstSession;
Calendar lastSession;

Random random;

// with what likelihood are we going to offer
// the reducer an uninteresting instance for a given date
private float THRESHOLD = 1f;

@Override
public void map(LongWritable key, ObjectWritable obj, Context context) throws IOException,
        InterruptedException
{
    Instance instance;
    String classLabel;
    Calendar session;
    Configuration conf = context.getConfiguration();

    if (setupFailedException != null)
    {
        throw new IOException(setupFailedException);
    }

    FileSplit file = (FileSplit) context.getInputSplit();

    // Extract date from path
    String date = file.getPath().getParent().getName();

    instance = (Instance) obj.get();

    classLabel = instance.getClassLabel();

    if (interestingUsers.containsKey(classLabel))
    {
        k.set("INTERESTING_CLASS\t" + classLabel + "\t" + date);
        v.set(instance.toString());
        context.write(k, v);
    }
    else
    {
        // do magic

        session = (Calendar) firstSession.clone();

        if (Arrays.binarySearch(trainingClasses, classLabel) >= 0)
        {
            k.set("UNINTERESTING_CLASS\tadditionalTrainingInstances");
            v.set(instance.toString());
            context.write(k, v);
        }
        if (Arrays.binarySearch(testClasses, classLabel) >= 0)
        {
            k.set("UNINTERESTING_CLASS\tadditionalTestInstances");
            v.set(instance.toString());
            context.write(k, v);
        }
    }
}

@Override
public void setup(Context context)
{
    Configuration conf = context.getConfiguration();

    random = new Random(1); // TODO �berlegen ob statischer Seed gut ist

    Util.populateBlackOrWhiteList(conf, interestingUsers, Util.CONF_OPEN_WORLD_USERLIST_PATH);

    try
    {
        ClassIndex classIndex = ClassIndex.getInstance();
        List<String> list;

        if (!classIndex.isPopulated())
        {
            classIndex.init(conf);
            classIndex.populateIndex();
        }

        int numTrainingClasses = Util.getInt(conf, Util.CONF_OPEN_WORLD_NUM_TRAINING_INSTANCES);
        int numTestClasses = Util.getInt(conf, Util.CONF_OPEN_WORLD_NUM_TEST_INSTANCES);

        list = classIndex.asArrayList();
        list.removeAll(interestingUsers.keySet());
        list = Util.getRandomSubList(list, numTrainingClasses, random);
        Collections.sort(list);
        trainingClasses = list.toArray(new String[numTrainingClasses]);

        list = classIndex.asArrayList();
        list.removeAll(interestingUsers.keySet());
        list = Util.getRandomSubList(list, numTestClasses, random);
        Collections.sort(list);
        testClasses = list.toArray(new String[numTestClasses]);

        firstSession = Util.getCalendarFromString(conf.get(Util.CONF_FIRST_SESSION));
        lastSession = Util.getCalendarFromString(conf.get(Util.CONF_LAST_SESSION));
    } catch (Exception e)
    {
        e.printStackTrace();
    }
}
}