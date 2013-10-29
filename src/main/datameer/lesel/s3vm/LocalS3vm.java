package datameer.lesel.s3vm;

import java.util.Collections;
import java.util.List;

import datameer.lesel.core.Instance;
import datameer.lesel.core.Instances;

public class LocalS3vm {

	private Instances _data;
	private double _b;
	private double _lambda;
	private double[] _w;

	public LocalS3vm(Instances instances) {
		_data = instances;
	}

	public void run(double lambda, double eta0) {
		normalizeInstances();

		runSgd(lambda, eta0);
	}

	private void runSgd(double lambda, double eta0) {
		_lambda = lambda;

		List<Instance> shuffledData = _data.getAsList();
		Collections.shuffle(shuffledData);

		int t = 0;
		_w = new double[_data.getNumberOfAttributes()];
		for (Instance instance : shuffledData) {
			double eta = eta0 / (1.0 + _lambda * eta0 * t);

			double margin = _b;
			double[] x = instance.getX();
			for (int i = 0; i < x.length; i++) {
				margin += _w[i] * x[i];
			}

			if (instance.hasLabel()) {
				double label = instance.getY();
				if (Math.signum(label) != Math.signum(margin)) {
					for (int i = 0; i < x.length; i++) {
						_w[i] -= label * eta * x[i];
					}
				}
			} else {
				for (int i = 0; i < x.length; i++) {
					_w[i] += eta * 6.0 * Math.exp(-3.0 * (margin + _b) * (margin + _b)) * (margin + _b) * x[i];
				}
			}

			for (int i = 0; i < x.length; i++) {
				_w[i] -= eta * _lambda * _w[i];
			}

			t++;
		}

	}

	private void normalizeInstances() {
		double[] unlabeledMeans = new double[_data.getNumberOfAttributes()];
		double labelsMean = 0.0;

		long unlabeledCount = 0;
		long labeledCount = 0;
		for (Instance instance : _data) {
			if (instance.hasLabel()) {
				labelsMean += instance.getY();
				labeledCount++;
			} else {
				for (int i = 0; i < _data.getNumberOfAttributes(); i++) {
					unlabeledMeans[i] += instance.getX()[i];
				}
				unlabeledCount++;
			}
		}

		for (int i = 0; i < _data.getNumberOfAttributes(); i++) {
			unlabeledMeans[i] /= unlabeledCount;
		}

		for (Instance instance : _data) {
			if (!instance.hasLabel()) {
				for (int i = 0; i < _data.getNumberOfAttributes(); i++) {
					instance.getX()[i] -= unlabeledMeans[i];
				}
			}
		}

		_b = labelsMean / labeledCount;
	}

	public static void main(String[] args) throws Exception {
		String inputFilePath = args[0];

		Instances data = Instances.loadFromFile(inputFilePath);

		LocalS3vm localS3vm = new LocalS3vm(data);
		localS3vm.run(1.0, 10.0);

	}

}
