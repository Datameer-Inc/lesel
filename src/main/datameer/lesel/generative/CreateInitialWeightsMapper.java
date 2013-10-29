package datameer.lesel.generative;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import datameer.lesel.core.Instance;

public class CreateInitialWeightsMapper extends Mapper<LongWritable, Text, NullWritable, InstanceWithWeightsWritable> {
	private Random _random = new Random(11);
	private ExpectationComputation _expectationComputation;

	public CreateInitialWeightsMapper() {
		_expectationComputation = new ExpectationComputation(null);
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		Instance instance = Instance.createFromText(line);

		double randomNumber = _random.nextDouble();
		double t[] = new double[] { randomNumber, 1.0 - randomNumber };

		_expectationComputation.updateWeightSums(t);

		InstanceWithWeightsWritable returnValue = new InstanceWithWeightsWritable(instance, t);

		context.write(NullWritable.get(), returnValue);
	}
}
