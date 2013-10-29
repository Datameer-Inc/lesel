package datameer.lesel.s3vm;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import datameer.lesel.core.Instance;

public class InstanceMeans {
	private static final Logger LOG = Logger.getLogger(InstanceMeans.class);
	private static final int DEFAULT_FILE_SIZE = 20000;
	private static final Gson GSON = new Gson();

	private double[] _unlabeledInstancesMeans;
	private double _labelsMean;
	private long _labeledCount;
	private long _unlabeledCount;

	private InstanceMeans(int numberOfAtributes) {
		_unlabeledInstancesMeans = new double[numberOfAtributes];
	}

	public void updateMeans(Instance instance) {
		if (instance.hasLabel()) {
			_labelsMean += instance.getY();
			_labeledCount++;
		} else {
			for (int i = 0; i < instance.getX().length; i++) {
				_unlabeledInstancesMeans[i] += instance.getX()[i];
			}
			_unlabeledCount++;
		}
	}

	public void merge(InstanceMeans means) {
		for (int i = 0; i < _unlabeledInstancesMeans.length; i++) {
			_unlabeledInstancesMeans[i] += means._unlabeledInstancesMeans[i];
		}
		_labelsMean += means._labelsMean;

		_unlabeledCount += means._unlabeledCount;
		_labeledCount += means._labeledCount;
	}

	public double getLabelsMean() {
		return _labelsMean / _labeledCount;
	}

	public double[] getUnlabeledInstancesMeans() {
		double[] result = new double[_unlabeledInstancesMeans.length];

		for (int i = 0; i < _unlabeledInstancesMeans.length; i++) {
			result[i] = _unlabeledInstancesMeans[i] / _unlabeledCount;
		}

		return result;
	}

	public long getNumberOfInstances() {
		return _labeledCount + _unlabeledCount;
	}

	public static void writeMeansFile(Path meansPath, FileSystem fileSystem, InstanceMeans means) throws IOException {
		String overallStatisticsAsJson = GSON.toJson(means);

		FSDataOutputStream overallMeansStream = fileSystem.create(meansPath);
		try {
			overallMeansStream.write(overallStatisticsAsJson.getBytes());
		} finally {
			overallMeansStream.close();
		}
	}

	public static InstanceMeans readMeansFile(Path meansPath, FileSystem fileSystem) throws IOException {
		LOG.info("Reading means file from " + meansPath.toString());

		FSDataInputStream meansStream = fileSystem.open(meansPath);
		try {
			String meansJson = readTextFile(meansStream);
			InstanceMeans means = GSON.fromJson(meansJson, InstanceMeans.class);

			return means;
		} finally {
			meansStream.close();
		}
	}

	private static String readTextFile(FSDataInputStream statisticsStream) throws IOException {
		int bufferIndex = 0;
		byte[] buffer = new byte[DEFAULT_FILE_SIZE];
		while (true) {
			int numberBytesRead = statisticsStream.read(buffer, bufferIndex, buffer.length - bufferIndex);
			if (numberBytesRead >= buffer.length - bufferIndex) {
				bufferIndex += numberBytesRead;
				buffer = Arrays.copyOf(buffer, 2 * buffer.length);
			} else {
				bufferIndex += numberBytesRead;
				break;
			}
		}
		String text = new String(buffer, 0, bufferIndex);
		return text;
	}

	public static InstanceMeans create(int numberOfAttributes) {
		return new InstanceMeans(numberOfAttributes);
	}

	public static InstanceMeans createFromJson(String json) {
		return GSON.fromJson(json, InstanceMeans.class);
	}
}
