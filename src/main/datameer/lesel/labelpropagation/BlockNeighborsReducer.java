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

public class BlockNeighborsReducer extends Reducer<LongWritable, BlockInstanceWritable, NullWritable, InstanceWithNeighborsWritable> {
	public static int NUMBER_OF_NEIGHBORS = 4;

	protected void reduce(LongWritable key, Iterable<BlockInstanceWritable> values, Context context) throws IOException, InterruptedException {
		List<BlockInstanceWritable> instances = Lists.newArrayList();
		final List<BlockInstanceWritable> neighbors = Lists.newArrayList();

		for (BlockInstanceWritable instance : values) {
			if (instance.getNeighborStatus().equals(BlockInstanceWritable.NeighborStatus.IS_NEIGHBOR)) {
				neighbors.add(new BlockInstanceWritable(instance));
			} else {
				instances.add(new BlockInstanceWritable(instance));
			}
		}

		for (int i = 0; i < instances.size(); i++) {
			findNearestNeighbors(context, instances, neighbors, i);
		}
	}

	private void findNearestNeighbors(Context context, List<BlockInstanceWritable> instances, final List<BlockInstanceWritable> neighbors, int i)
			throws IOException, InterruptedException {
		final Instance instance = instances.get(i).getInstance();
		long id = instances.get(i).getId();

		List<Integer> neighborIndices = findNearestNeighbors(neighbors, instance, id);

		List<Long> neighborIds = Lists.newArrayList();
		List<Double> distances = Lists.newArrayList();
		for (Integer index : neighborIndices) {
			neighborIds.add(neighbors.get(index).getId());
			distances.add(Instance.euclidianDistance(instance, neighbors.get(index).getInstance()));
		}

		InstanceWithNeighborsWritable neighborsWritable = new InstanceWithNeighborsWritable(instance, id, distances, neighborIds, instance.getY());
		context.write(NullWritable.get(), neighborsWritable);
	}

	private List<Integer> findNearestNeighbors(final List<BlockInstanceWritable> neighbors, final Instance instance, long id) {
		Comparator<Integer> comparator = new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				double distance1 = Instance.euclidianDistance(instance, neighbors.get(o1).getInstance());
				double distance2 = Instance.euclidianDistance(instance, neighbors.get(o2).getInstance());

				return Double.compare(distance1, distance2);
			}
		};

		MinMaxPriorityQueue<Integer> queue = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(NUMBER_OF_NEIGHBORS).create();

		for (int j = 0; j < neighbors.size(); j++) {
			if (id != neighbors.get(j).getId()) {
				if (queue.size() < NUMBER_OF_NEIGHBORS || comparator.compare(queue.peekLast(), j) > 0) {
					queue.add(j);
				}
			}
		}
		return Lists.newArrayList(queue.iterator());
	}
}