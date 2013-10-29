package datameer.lesel.generative;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import datameer.lesel.core.Instance;

public class ExpectationMapper extends Mapper<NullWritable, InstanceWithWeightsWritable, NullWritable, InstanceWithWeightsWritable> {
	public static final String STATISTICS_PATH_CONFIG = "statisticsPath";
	private MixtureModel _model;

	public ExpectationMapper() {
	}

	// for testing
	public ExpectationMapper(MixtureModel model) {
		_model = model;
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		String statisticsPath = context.getConfiguration().get(STATISTICS_PATH_CONFIG);
		MomentStatistics[] statistics = readStatisticsFromJson(context, statisticsPath);

		_model = MixtureModel.createFromStatistics(statistics);
	}

	private MomentStatistics[] readStatisticsFromJson(Context context, String statisticsPathString) throws IOException {
		Path statisticsPath = new Path(statisticsPathString);
		FileSystem fileSystem = statisticsPath.getFileSystem(context.getConfiguration());

		MomentStatistics[] statistics = MomentStatistics.readStatisticsFile(statisticsPath, fileSystem);

		return statistics;
	}

	public double[] computeNewWeights(Instance instance) {
		double[] x = instance.getX();
		double y = instance.getY();

		double[] t = new double[MixOfGaussians.NUMBER_OF_COMPONENTS];

		if (y != 0.0) {
			int targetIndex = y > 0.0 ? 0 : 1;
			t[targetIndex] = 1.0;
			t[1 - targetIndex] = 0.0;
		} else {
			computeNormalizedLikelihood(x, t);
		}

		return t;
	}

	private void computeNormalizedLikelihood(double[] x, double[] t) {
		double likelihood[] = new double[] { _model.computeLikelihood(x, 0), _model.computeLikelihood(x, 1) };
		double[] tau = _model.getTau();
		double normalization = tau[0] * likelihood[0] + tau[1] * likelihood[1];

		t[0] = tau[0] * likelihood[0] / normalization;
		t[1] = tau[1] * likelihood[1] / normalization;
	}

	protected void map(NullWritable key, InstanceWithWeightsWritable value, Context context) throws IOException, InterruptedException {
		Instance instance = value.getInstance();
		double[] t = computeNewWeights(instance);

		InstanceWithWeightsWritable result = new InstanceWithWeightsWritable(instance, t);

		context.write(NullWritable.get(), result);
	}
}
