package datameer.lesel.generative;

import java.util.Random;

import org.apache.log4j.Logger;

import datameer.lesel.core.Instances;

public class LocalMixOfGaussians {
	private static final Logger LOG = Logger.getLogger(LocalMixOfGaussians.class);

	private int NUMBER_OF_COMPONENTS = 2;

	private Instances _instances;
	private int _numberOfAttributes;
	private int _numberOfInstances;
	private int _iterations;

	private double[][] _mu;
	private double[][] _sigma;
	private double[] _tau;
	private double[][] _t;

	public LocalMixOfGaussians(Instances data) {
		_numberOfInstances = data.getSize();
		_numberOfAttributes = data.getNumberOfAttributes();

		_instances = data;

		_mu = new double[NUMBER_OF_COMPONENTS][_numberOfAttributes];
		_sigma = new double[NUMBER_OF_COMPONENTS][_numberOfAttributes];
		_tau = new double[NUMBER_OF_COMPONENTS];
		_t = new double[NUMBER_OF_COMPONENTS][_numberOfInstances];
	}

	void execute() {
		_iterations = 0;

		initialize();

		while (!hasConverged()) {
			computeMaximization();
			computeExpectation();

			_iterations++;
		}
	}

	private void initialize() {
		Random random = new Random(13);

		for (int i = 0; i < _numberOfInstances; i++) {
			_t[0][i] = random.nextDouble();
			_t[1][i] = 1.0 - _t[0][i];
		}

	}

	private void computeMaximization() {
		double[] sumOfT = new double[NUMBER_OF_COMPONENTS];
		double[][] newMu = new double[NUMBER_OF_COMPONENTS][_numberOfAttributes];
		double[][] newSigma = new double[NUMBER_OF_COMPONENTS][_numberOfAttributes];

		for (int i = 0; i < _numberOfInstances; i++) {
			sumOfT[0] += _t[0][i];
			sumOfT[1] += _t[1][i];

			for (int j = 0; j < _numberOfAttributes; j++) {
				newMu[0][j] += _t[0][i] * _instances.getInstance(i).getX()[j];
				newMu[1][j] += _t[1][i] * _instances.getInstance(i).getX()[j];
			}
		}

		for (int j = 0; j < _numberOfAttributes; j++) {
			newMu[0][j] /= sumOfT[0];
			newMu[1][j] /= sumOfT[1];
		}

		for (int i = 0; i < _numberOfInstances; i++) {
			for (int j = 0; j < _numberOfAttributes; j++) {
				double diff = _instances.getInstance(i).getX()[j] - newMu[0][j];
				newSigma[0][j] += _t[0][i] * diff * diff;

				diff = _instances.getInstance(i).getX()[j] - newMu[1][j];
				newSigma[1][j] += _t[1][i] * diff * diff;
			}
		}

		for (int j = 0; j < _numberOfAttributes; j++) {
			newSigma[0][j] /= sumOfT[0];
			newSigma[1][j] /= sumOfT[1];
		}

		for (int j = 0; j < _numberOfAttributes; j++) {
			_mu[0][j] = newMu[0][j];
			_mu[1][j] = newMu[1][j];
			_sigma[0][j] = newSigma[0][j];
			_sigma[1][j] = newSigma[1][j];
		}

		_tau[0] = sumOfT[0] / _numberOfInstances;
		_tau[1] = sumOfT[1] / _numberOfInstances;
	}

	private void computeExpectation() {
		for (int i = 0; i < _numberOfInstances; i++) {
			double target = _instances.getInstance(i).getY();
			if (target != 0.0) {
				int targetIndex = target > 0.0 ? 0 : 1;
				_t[targetIndex][i] = 1.0;
				_t[1 - targetIndex][i] = 0.0;
			} else {
				double[] x = _instances.getInstance(i).getX();
				double likelihood[] = new double[] { computeGaussian(x, _mu[0], _sigma[0]), computeGaussian(x, _mu[1], _sigma[1]) };
				double normalization = _tau[0] * likelihood[0] + _tau[1] * likelihood[1];

				_t[0][i] = _tau[0] * likelihood[0] / normalization;
				_t[1][i] = _tau[1] * likelihood[1] / normalization;
			}
		}
	}

	private double computeGaussian(double[] x, double[] mu, double[] sigma) {
		double exponent = 0.0;
		for (int i = 0; i < _numberOfAttributes; i++) {
			double diff = x[i] - mu[i];
			exponent += diff * diff / sigma[i];
		}
		exponent *= -0.5;

		double quotient = 1.0;
		for (int i = 0; i < _numberOfAttributes; i++) {
			quotient *= sigma[i] * 2 * Math.PI;
		}
		quotient = Math.sqrt(quotient);

		double result = Math.exp(exponent) / quotient;

		return result;
	}

	private boolean hasConverged() {
		if (_iterations > 10) {
			return true;
		}

		return false;
	}

	public double[][] getMu() {
		return _mu;
	}

	public double[][] getSigma() {
		return _sigma;
	}

	public double[] getTau() {
		return _tau;
	}

	public static void main(String[] args) throws Exception {
		String inputFilePath = args[0];

		Instances data = Instances.loadFromFile(inputFilePath);

		LocalMixOfGaussians mixOfGaussians = new LocalMixOfGaussians(data);
		mixOfGaussians.execute();

		LOG.info("Mu: " + mixOfGaussians.getMu());
		LOG.info("Sigma: " + mixOfGaussians.getSigma());
		LOG.info("Tau: " + mixOfGaussians.getTau());

	}
}
