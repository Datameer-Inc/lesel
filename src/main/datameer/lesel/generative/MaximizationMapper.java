package datameer.lesel.generative;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import datameer.lesel.core.Instance;

public class MaximizationMapper extends Mapper<NullWritable, InstanceWithWeightsWritable, NullWritable, Text> {
	private static final Logger LOG = Logger.getLogger(MaximizationMapper.class);

	public static final String NUMBER_OF_ATTRIBUTES_CONFIG = "NumberOfAttributes";

	private MomentStatistics[] _statistics;

	public MaximizationMapper() {
	}

	private void initStatistics(int numberOfAttributes) {
		_statistics = new MomentStatistics[MixOfGaussians.NUMBER_OF_COMPONENTS];
		_statistics[0] = MomentStatistics.createEmptyStatistics(numberOfAttributes);
		_statistics[1] = MomentStatistics.createEmptyStatistics(numberOfAttributes);
	}

	protected void map(NullWritable key, InstanceWithWeightsWritable value, Context context) throws IOException, InterruptedException {
		Instance instance = value.getInstance();

		if (_statistics == null) {
			initStatistics(instance.getX().length);
		}

		double[] weights = value.getWeights();
		for (int i = 0; i < weights.length; i++) {
			_statistics[i].update(instance, weights[i]);
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		Gson gson = new Gson();
		String sumsAsJson = gson.toJson(_statistics);

		LOG.info("Writing statistics for " + _statistics[0]._n + " input instances.");
		context.write(NullWritable.get(), new Text(sumsAsJson));
	}

}
