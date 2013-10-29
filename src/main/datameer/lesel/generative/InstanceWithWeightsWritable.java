package datameer.lesel.generative;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import datameer.lesel.core.DoubleArrayWritable;
import datameer.lesel.core.Instance;

public class InstanceWithWeightsWritable implements Writable {

	private DoubleArrayWritable _instance;
	private DoubleArrayWritable _weights;

	public InstanceWithWeightsWritable(Instance instance, double[] weights) {
		_instance = instance.getAsArray();
		_weights = new DoubleArrayWritable(weights);
	}

	public InstanceWithWeightsWritable() {
		_instance = new DoubleArrayWritable();
		_weights = new DoubleArrayWritable();
	}

	public InstanceWithWeightsWritable(DoubleArrayWritable instance, DoubleArrayWritable weights) {
		_instance = instance;
		_weights = weights;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		_instance.write(out);
		_weights.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_instance.readFields(in);
		_weights.readFields(in);
	}

	public Instance getInstance() {
		return Instance.createFromArray(_instance);
	}

	public double[] getWeights() {
		return _weights.getAsNativeArray();
	}

}
