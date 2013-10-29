package datameer.lesel.s3vm;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SgdMapper extends Mapper<NullWritable, InstanceWritable, DoubleWritable, InstanceWritable> {
	private Random _random = new Random(11);
	private DoubleWritable _outputKey = new DoubleWritable();

	public void map(NullWritable key, InstanceWritable value, Context context) throws IOException, InterruptedException {
		_outputKey.set(_random.nextDouble());
		context.write(_outputKey, value);
	}

}
