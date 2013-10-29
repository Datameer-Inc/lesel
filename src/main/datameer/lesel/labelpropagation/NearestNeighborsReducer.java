package datameer.lesel.labelpropagation;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;

import datameer.lesel.core.Instance;

public class NearestNeighborsReducer extends Reducer<LongWritable, InstanceWithNeighborsWritable, NullWritable, InstanceWithNeighborsWritable> {

	protected void reduce(LongWritable key, Iterable<InstanceWithNeighborsWritable> values, Context context) throws IOException, InterruptedException {
		final List<Long> ids = Lists.newArrayList();
		final List<Double> distances = Lists.newArrayList();
		Instance instance = null;
		long id = -1;
		double label = 0.0;

		for (InstanceWithNeighborsWritable nearestNeighbors : values) {
			ids.addAll(nearestNeighbors.getNeighborIds());
			distances.addAll(nearestNeighbors.getDistances());

			if (instance == null) {
				instance = nearestNeighbors.getInstance();
				id = nearestNeighbors.getId();
				label = nearestNeighbors.getLabel();
			}
		}

		List<Integer> neighbors = identifyNearestNeighbors(ids, distances);

		List<Long> neighborIds = Lists.newArrayList();
		List<Double> neighborDistances = Lists.newArrayList();
		for (Integer index : neighbors) {
			neighborIds.add(ids.get(index));
			neighborDistances.add(distances.get(index));
		}

		InstanceWithNeighborsWritable result = new InstanceWithNeighborsWritable(instance, id, neighborDistances, neighborIds, label);
		context.write(NullWritable.get(), result);
	}

	private List<Integer> identifyNearestNeighbors(final List<Long> ids, final List<Double> distances) {
		Comparator<Integer> comparator = new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return Double.compare(distances.get(o1), distances.get(o2));
			}
		};
		MinMaxPriorityQueue<Integer> queue = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(BlockNeighborsReducer.NUMBER_OF_NEIGHBORS).create();

		for (int i = 0; i < ids.size(); i++) {
			queue.add(i);
		}
		return Lists.newArrayList(queue);
	}
}