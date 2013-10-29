package datameer.lesel.labelpropagation;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;

import datameer.lesel.core.Instance;
import datameer.lesel.core.Instances;

public class LocalLabelPropagation {

	private static final int NUMBER_OF_ITERATIONS = 10;
	private static int NUMBER_OF_NEIGHBORS = 4;
	private Instances _data;
	private List<NearestNeighbors> _neighbors;
	private double _mu = 0.2 / (1.0 - 0.2);
	private double _epsilon = 1e-4;

	public LocalLabelPropagation(Instances data) {
		_data = data;
	}

	public void run() {

		_neighbors = computeNearestNeighbors();

		runLabelPropagation();
	}

	private void runLabelPropagation() {
		List<Double> labels = Lists.newArrayList();
		List<Instance> instances = _data.getAsList();
		for (int i = 0; i < instances.size(); i++) {
			labels.add(instances.get(i).getY());
		}

		for (int i = 0; i < NUMBER_OF_ITERATIONS; i++) {
			runLabelPropagationIteration(labels);
		}
	}

	private void runLabelPropagationIteration(List<Double> labels) {
		List<Instance> instances = _data.getAsList();

		for (int i = 0; i < instances.size(); i++) {
			List<Integer> neighbors = _neighbors.get(i).getNeighbors();

			double neighborsLabelsSum = 0.0;
			for (Integer neighborIndex : neighbors) {
				neighborsLabelsSum += labels.get(neighborIndex);
			}

			double newLabel = 0.0;
			if (instances.get(i).hasLabel()) {
				newLabel = neighborsLabelsSum + instances.get(i).getY() / _mu;
				newLabel /= NUMBER_OF_NEIGHBORS + 1.0 / _mu + _epsilon;
			} else {
				newLabel = neighborsLabelsSum / (NUMBER_OF_NEIGHBORS + 1.0 / _epsilon);
			}
			labels.set(i, newLabel);
		}
	}

	private List<NearestNeighbors> computeNearestNeighbors() {
		final List<Instance> instances = _data.getAsList();
		List<NearestNeighbors> result = Lists.newArrayList();

		for (int i = 0; i < instances.size(); i++) {
			final Integer instanceIndex = i;

			Comparator<Integer> comparator = new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					double distance1 = Instance.euclidianDistance(instances.get(instanceIndex), instances.get(o1));
					double distance2 = Instance.euclidianDistance(instances.get(instanceIndex), instances.get(o2));

					return Double.compare(distance1, distance2);
				}
			};

			MinMaxPriorityQueue<Integer> queue = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(NUMBER_OF_NEIGHBORS).create();
			for (int j = 0; j < instances.size(); j++) {
				if (j != i && (queue.size() < NUMBER_OF_NEIGHBORS || comparator.compare(queue.peekLast(), j) > 0)) {
					queue.add(j);
				}
			}

			NearestNeighbors neighbors = new NearestNeighbors(queue);
			result.add(neighbors);
		}

		return result;
	}

	public static void main(String[] args) throws Exception {
		String inputFilePath = args[0];

		Instances data = Instances.loadFromFile(inputFilePath);

		LocalLabelPropagation localLabelPropagation = new LocalLabelPropagation(data);
		localLabelPropagation.run();

	}

	class NearestNeighbors {
		private List<Integer> _neighbors = Lists.newArrayList();

		public NearestNeighbors(Collection<Integer> neighbors) {
			_neighbors.addAll(neighbors);
		}

		public List<Integer> getNeighbors() {
			return _neighbors;
		}
	}
}
