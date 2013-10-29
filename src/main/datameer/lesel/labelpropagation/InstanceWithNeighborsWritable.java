package datameer.lesel.labelpropagation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

import datameer.lesel.core.DoubleArrayWritable;
import datameer.lesel.core.Instance;
import datameer.lesel.core.LongArrayWritable;

public class InstanceWithNeighborsWritable implements Writable {
	private DoubleArrayWritable _instance;
	private LongWritable _id;
	private DoubleArrayWritable _distances;
	private LongArrayWritable _neighborIds;
	private DoubleWritable _label;

	public InstanceWithNeighborsWritable(Instance instance, long id, List<Double> distances, List<Long> neighborIds, Double label) {
		_instance = instance.getAsArray();
		_id = new LongWritable(id);
		_distances = new DoubleArrayWritable(distances);
		_neighborIds = new LongArrayWritable(neighborIds);
		_label = new DoubleWritable(label);
	}

	public InstanceWithNeighborsWritable() {
		_instance = new DoubleArrayWritable();
		_id = new LongWritable();
		_distances = new DoubleArrayWritable();
		_neighborIds = new LongArrayWritable();
		_label = new DoubleWritable();
	}

	public InstanceWithNeighborsWritable(DoubleArrayWritable instance, LongWritable id, DoubleArrayWritable distances, LongArrayWritable neighborIds,
			DoubleWritable label) {
		_instance = instance;
		_id = id;
		_distances = distances;
		_neighborIds = neighborIds;
		_label = label;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		_instance.write(out);
		_id.write(out);
		_distances.write(out);
		_neighborIds.write(out);
		_label.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_instance.readFields(in);
		_id.readFields(in);
		_distances.readFields(in);
		_neighborIds.readFields(in);
		_label.readFields(in);
	}

	public Instance getInstance() {
		return Instance.createFromArray(_instance);
	}

	public long getId() {
		return _id.get();
	}

	public List<Double> getDistances() {
		List<Double> result = Lists.newArrayList();

		for (Writable distanceWritable : _distances.get()) {
			result.add(((DoubleWritable) distanceWritable).get());
		}

		return result;
	}

	public List<Long> getNeighborIds() {
		List<Long> result = Lists.newArrayList();

		for (Writable neighborWritable : _neighborIds.get()) {
			result.add(((LongWritable) neighborWritable).get());
		}

		return result;
	}

	public Double getLabel() {
		return _label.get();
	}
}
