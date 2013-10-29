package datameer.lesel.generative;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

public class ExpectationComputation {
	private static final Logger LOG = Logger.getLogger(ExpectationComputation.class);

	public static final String SUMS_OF_WEIGHTS_DIRECTORY_NAME = "sums";
	public static final String SUM_FILENAME_PREFIX = "sum_";

	private MixtureModel _model;
	private double _sumOfT[];

	public ExpectationComputation(MixtureModel model) {
		_model = model;
		_sumOfT = new double[MixOfGaussians.NUMBER_OF_COMPONENTS];
	}

	public void saveSumsOfWeights(Path outputPath, TaskID taskId) throws IOException, InterruptedException {
		Path sumsFilePath = new Path(outputPath, SUMS_OF_WEIGHTS_DIRECTORY_NAME + "/" + SUM_FILENAME_PREFIX + taskId.toString());
		LOG.info("Write sums of weights to " + sumsFilePath.toString());

		Configuration conf = new Configuration();
		FileSystem fs = sumsFilePath.getFileSystem(conf);

		Gson gson = new Gson();
		String sumsAsJson = gson.toJson(_sumOfT);

		FSDataOutputStream sumsStream = fs.create(sumsFilePath);
		try {
			sumsStream.write(sumsAsJson.getBytes());
		} finally {
			sumsStream.close();
		}
	}

	public void updateWeightSums(double[] t) {
		_sumOfT[0] += t[0];
		_sumOfT[1] += t[1];
	}
}
