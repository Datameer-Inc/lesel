package datameer.lesel.generative;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import datameer.lesel.core.Instance;

public class MomentStatistics {
	private static final Logger LOG = Logger.getLogger(MomentStatistics.class);
	private static final int DEFAULT_FILE_SIZE = 20000;
	private static final Gson GSON = new Gson();

	public long _n;
	public double _weightSum;
	public double[] _means;
	public double[] _m2;

	private MomentStatistics(int numberOfAttributes) {
		_n = 0;
		_weightSum = 0.0;
		_means = new double[numberOfAttributes];
		_m2 = new double[numberOfAttributes];
	}

	public void update(Instance instance, double weight) {
		_n++;
		double newWeightSum = _weightSum + weight;

		for (int i = 0; i < _means.length; i++) {
			double delta = instance.getX()[i] - _means[i];
			double weightedDelta = delta * weight / newWeightSum;
			_means[i] += weightedDelta;
			_m2[i] += _weightSum * delta * weightedDelta;
		}

		_weightSum = newWeightSum;
	}

	public ArrayWritable getAsArray(int componentId, int attributeNumber) {
		DoubleWritable componentIdWritable = new DoubleWritable((double) componentId);
		DoubleWritable attributeNumberWritable = new DoubleWritable((double) attributeNumber);
		DoubleWritable weightSum = new DoubleWritable(_weightSum);
		DoubleWritable mean = new DoubleWritable(_means[attributeNumber]);
		DoubleWritable m2 = new DoubleWritable(_m2[attributeNumber]);

		return new ArrayWritable(DoubleWritable.class, new Writable[] { componentIdWritable, attributeNumberWritable, weightSum, mean, m2 });
	}

	public void merge(MomentStatistics momentStatistics) {
		_n += momentStatistics._n;
		double newWeightSum = _weightSum + momentStatistics._weightSum;

		int numberOfAttributes = _means.length;
		double[] newMeans = new double[numberOfAttributes];
		double[] newM2 = new double[numberOfAttributes];

		for (int i = 0; i < _means.length; i++) {
			double delta = momentStatistics._means[i] - _means[i];
			newMeans[i] = _means[i] + delta * momentStatistics._weightSum / newWeightSum;
			newM2[i] = _m2[i] + momentStatistics._m2[i] + delta * delta * _weightSum * momentStatistics._weightSum / newWeightSum;
		}

		_weightSum = newWeightSum;
		_means = newMeans;
		_m2 = newM2;
	}

	public static MomentStatistics createEmptyStatistics(int numberOfAttributes) {
		return new MomentStatistics(numberOfAttributes);
	}

	public static void writeStatisticsFile(Path statisticsPath, FileSystem fileSystem, MomentStatistics[] statistics) throws IOException {
		String overallStatisticsAsJson = GSON.toJson(statistics);

		FSDataOutputStream overallStatisticsStream = fileSystem.create(statisticsPath);
		try {
			overallStatisticsStream.write(overallStatisticsAsJson.getBytes());
		} finally {
			overallStatisticsStream.close();
		}
	}

	public static MomentStatistics[] readStatisticsFile(Path statisticsPath, FileSystem fileSystem) throws IOException {
		LOG.info("Reading statistics file " + statisticsPath.getName());

		FSDataInputStream statisticsStream = fileSystem.open(statisticsPath);
		try {
			String statisticsJson = readTextFile(statisticsStream);
			MomentStatistics[] statistics = GSON.fromJson(statisticsJson, MomentStatistics[].class);

			return statistics;
		} finally {
			statisticsStream.close();
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
}