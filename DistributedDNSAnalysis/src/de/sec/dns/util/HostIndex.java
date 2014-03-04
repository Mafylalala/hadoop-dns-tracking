package de.sec.dns.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import de.sec.dns.dataset.DataSetHeader;
import de.sec.dns.dataset.InstanceRecordReader;

/**
 * The host index. This class provides access to a list of all hosts and their
 * numeric index. This class is a singleton.
 * 
 * @author Christian Banse, Dominik Herrmann
 */
public class HostIndex {
	/**
	 * The singleton.
	 */
	private static HostIndex _hostIndex;

	/**
	 * A logger.
	 */
	private static final Log LOG = LogFactory
			.getLog(InstanceRecordReader.class);

	/**
	 * Retrieves or creates the instance of this class.
	 * 
	 * @return The instance of <i>ClassIndex</i>.
	 */
	public static HostIndex getInstance() {
		if (_hostIndex == null) {
			_hostIndex = new HostIndex();
		}

		return _hostIndex;
	}

	/**
	 * The frequencies of the hosts. TODO maybe
	 */
	// private int[] frequencies = null;

	/**
	 * A cached <i>TaskAttemptContext</i>.
	 */
	private TaskAttemptContext context;

	/**
	 * The host names.
	 */
	FlatMap hostNames = null;

	/**
	 * Returns the host name of the specified index.
	 * 
	 * @param i
	 *            The desired index.
	 * @return The class name of the index.
	 */
	public CompactCharSequence getHostname(int i) {

		return new CompactCharSequence(hostNames.getValueFromPosition(i));
	}

	/**
	 * Returns index position of the host name in the index or a value < 0 if
	 * the given class name is not contained in the index. It uses a binary
	 * search.
	 * 
	 * @param md5Host
	 *            The host name.
	 * @return The index of the host name.
	 */
	public int getIndexPosition(CompactCharSequence md5Host) {
		return hostNames.get(md5Host.getBytes());
	}

	/**
	 * Returns the size of the index.
	 * 
	 * @return The size of the index.
	 */
	public int getSize() {
		return hostNames.size();
	}

	/**
	 * Initializes the host index with the given context.
	 * 
	 * @param context
	 *            A <i>TaskAttemptContext</i>.
	 * @throws Exception
	 */
	public void init(TaskAttemptContext context) throws Exception {
		if (this.context == null) {
			this.context = context;
		} else {
			throw new Exception(
					"init has already been called; cannot initialize this index again");
		}
	}

	/**
	 * Returns if the index is already populated.
	 * 
	 * @return True if the index is populated, false otherwise.
	 */
	public boolean isPopulated() {
		return (hostNames != null); // && (frequencies != null);
	}

	/**
	 * Populates the index.
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void populateIndex() throws Exception {
		FileSystem fs = FileSystem.get(context.getConfiguration());

		// Path path = new Path(context.getConfiguration().get(
		// Util.CONF_HEADER_PATH));

		LOG.info("starting to populate the index...");

		Util.getMemoryInfo(HostIndex.class);

		hostNames = new FlatMap(new Path(context.getConfiguration().get(
				Util.CONF_HEADER_PATH)
				+ "/" + DataSetHeader.HOST_INDEX_DIRECTORY), fs);

		LOG.info("populateIndex done");
		Util.getMemoryInfo(HostIndex.class);
	}
}
