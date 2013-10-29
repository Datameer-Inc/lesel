package datameer.lesel.labelpropagation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import datameer.lesel.core.Instance;

public class LabelPropagationWritable implements Writable {

	private BooleanWritable _isInstance;
	private InstanceWithNeighborsWritable _instance;
	private DoubleWritable _label;

	public LabelPropagationWritable(Instance instance, long id, List<Double> distances, List<Long> neighborIds, Double label) {
		_isInstance = new BooleanWritable(true);
		_instance = new InstanceWithNeighborsWritable(instance, id, distances, neighborIds, label);
	}

	public LabelPropagationWritable(Double label) {
		_isInstance = new BooleanWritable(false);
		_label = new DoubleWritable(label);
	}

	public LabelPropagationWritable(InstanceWithNeighborsWritable instance) {
		_isInstance = new BooleanWritable(true);
		_instance = instance;
	}

	public LabelPropagationWritable() {
		_isInstance = new BooleanWritable();
		_instance = new InstanceWithNeighborsWritable();
		_label = new DoubleWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		_isInstance.write(out);

		if (_isInstance.get()) {
			_instance.write(out);
		} else {
			_label.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_isInstance.readFields(in);

		if (_isInstance.get()) {
			_instance.readFields(in);
		} else {
			_label.readFields(in);
		}
	}

	public boolean isInstance() {
		return _isInstance.get();
	}

	public InstanceWithNeighborsWritable getInstance() {
		return _instance;
	}

	public Double getLabel() {
		return _label.get();
	}
}