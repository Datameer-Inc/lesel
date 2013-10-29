package datameer.lesel.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class Instances implements Iterable<Instance> {
	private static final Logger LOG = Logger.getLogger(Instances.class);

	private List<Instance> _instances = Lists.newArrayList();

	private Instances() {
	}

	public int getSize() {
		return _instances.size();
	}

	public int getNumberOfAttributes() {
		Preconditions.checkState(getSize() > 0, "Cannot get number of attributes: data set is empty");

		return _instances.get(0).getX().length;
	}

	public void addInstance(Instance instance) {
		_instances.add(instance);
	}

	public Instance getInstance(int index) {
		return _instances.get(index);
	}

	@Override
	public Iterator<Instance> iterator() {
		return _instances.iterator();
	}

	public List<Instance> getAsList() {
		return _instances;
	}

	static public Instances copyOf(Instances source) {
		Instances result = new Instances();

		for (Instance instance : source) {
			result._instances.add(Instance.copyOf(instance));
		}

		return result;
	}

	static public Instances loadFromFile(String path) {
		Instances instances = new Instances();

		try {
			File inputFile = new File(path);
			InputStream inputStream = new FileInputStream(inputFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
			try {
				String line;
				while ((line = reader.readLine()) != null) {
					instances.addInstance(Instance.createFromText(line));
				}
			} finally {
				reader.close();
			}
		} catch (Exception e) {
			LOG.error(e);
		}

		return instances;
	}

}
