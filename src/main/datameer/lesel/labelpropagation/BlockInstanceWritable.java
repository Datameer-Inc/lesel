package datameer.lesel.labelpropagation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import datameer.lesel.core.DoubleArrayWritable;
import datameer.lesel.core.Instance;

public class BlockInstanceWritable implements Writable {
	public static enum NeighborStatus {
		IS_INSTANCE, IS_NEIGHBOR
	};

	private LongWritable _id;
	private DoubleArrayWritable _instance;
	private BooleanWritable _isNeighbor;

	public BlockInstanceWritable(long id, Instance instance, NeighborStatus status) {
		_id = new LongWritable(id);
		_instance = instance.getAsArray();
		_isNeighbor = new BooleanWritable(status == NeighborStatus.IS_NEIGHBOR);
	}

	public BlockInstanceWritable() {
		_id = new LongWritable();
		_instance = new DoubleArrayWritable();
		_isNeighbor = new BooleanWritable();
	}

	public BlockInstanceWritable(LongWritable id, DoubleArrayWritable instance, BooleanWritable status) {
		_id = id;
		_instance = instance;
		_isNeighbor = status;
	}

	public BlockInstanceWritable(BlockInstanceWritable source) {
		_id = new LongWritable(source._id.get());
		_instance = new DoubleArrayWritable(source._instance);
		_isNeighbor = new BooleanWritable(source._isNeighbor.get());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		_id.write(out);
		_instance.write(out);
		_isNeighbor.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_id.readFields(in);
		_instance.readFields(in);
		_isNeighbor.readFields(in);
	}

	public long getId() {
		return _id.get();
	}

	public Instance getInstance() {
		return Instance.createFromArray(_instance);
	}

	public NeighborStatus getNeighborStatus() {
		return _isNeighbor.get() ? NeighborStatus.IS_NEIGHBOR : NeighborStatus.IS_INSTANCE;
	}

}
