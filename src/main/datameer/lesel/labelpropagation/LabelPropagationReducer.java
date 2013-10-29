package datameer.lesel.labelpropagation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LabelPropagationReducer extends Reducer<LongWritable, LabelPropagationWritable, NullWritable, InstanceWithNeighborsWritable> {

	private double _mu = 0.2 / (1.0 - 0.2);
	private double _epsilon = 1e-4;

	protected void reduce(LongWritable key, Iterable<LabelPropagationWritable> values, Context context) throws IOException, InterruptedException {
		InstanceWithNeighborsWritable instance = null;
		double labelSum = 0.0;

		for (LabelPropagationWritable message : values) {
			if (message.isInstance()) {
				instance = message.getInstance();
			} else {
				labelSum += message.getLabel();
			}
		}

		double newLabel = 0.0;
		if (instance.getInstance().hasLabel()) {
			newLabel = labelSum + instance.getInstance().getY() / _mu;
			newLabel /= BlockNeighborsReducer.NUMBER_OF_NEIGHBORS + 1.0 / _mu + _epsilon;
		} else {
			newLabel = labelSum / (BlockNeighborsReducer.NUMBER_OF_NEIGHBORS + 1.0 / _epsilon);
		}

		context.write(NullWritable.get(),
				new InstanceWithNeighborsWritable(instance.getInstance(), instance.getId(), instance.getDistances(), instance.getNeighborIds(), newLabel));
	}
}