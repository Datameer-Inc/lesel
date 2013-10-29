package datameer.lesel.s3vm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import datameer.lesel.core.DoubleArrayWritable;
import datameer.lesel.core.Instance;

public class InstanceWritable implements Writable {

	private DoubleArrayWritable _instance;

	public InstanceWritable(Instance instance) {
		_instance = instance.getAsArray();
	}

	public InstanceWritable() {
		_instance = new DoubleArrayWritable();
	}

	public InstanceWritable(DoubleArrayWritable instance) {
		_instance = instance;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		_instance.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_instance.readFields(in);
	}

	public Instance getInstance() {
		return Instance.createFromArray(_instance);
	}

}