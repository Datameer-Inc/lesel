package datameer.lesel.generative;

import com.google.gson.Gson;

public class MixtureModel {
	private double[][] _mu;
	private double[][] _sigma;
	private double[] _tau;

	private MixtureModel(double[][] mu, double[][] sigma, double[] tau) {
		_mu = mu;
		_sigma = sigma;
		_tau = tau;
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

	public double computeLikelihood(double[] x, int componentId) {
		double exponent = 0.0;
		for (int i = 0; i < x.length; i++) {
			double diff = x[i] - _mu[componentId][i];
			exponent += diff * diff / _sigma[componentId][i];
		}
		exponent *= -0.5;

		double quotient = 1.0;
		for (int i = 0; i < x.length; i++) {
			quotient *= _sigma[componentId][i] * 2 * Math.PI;
		}
		quotient = Math.sqrt(quotient);

		double result = Math.exp(exponent) / quotient;

		return result;
	}

	public String getAsJson() {
		Gson gson = new Gson();

		return gson.toJson(this);
	}

	public String toString() {
		String result = "";
		for (int i = 0; i < _mu.length; i++) {
			int numberOfAttributes = _mu[i].length;

			result += "component " + i + ": [";
			result += "means: ";
			for (int j = 0; j < numberOfAttributes; j++) {
				result += _mu[i][j] + ((i < numberOfAttributes) ? ", " : " ");
			}

			result += "variances: ";
			for (int j = 0; j < numberOfAttributes; j++) {
				result += _sigma[i][j] + ((i < numberOfAttributes) ? ", " : "");
			}
			result += "]\n";
		}
		return result;
	}

	public static MixtureModel create(double[][] mu, double[][] sigma, double[] tau) {
		return new MixtureModel(mu, sigma, tau);
	}

	public static MixtureModel createFromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, MixtureModel.class);
	}

	public static MixtureModel createFromStatistics(MomentStatistics[] statistics) {
		int numberOfComponents = statistics.length;
		int numberOfAttributes = statistics[0]._means.length;

		double[][] mu = new double[numberOfComponents][numberOfAttributes];
		double[][] sigma = new double[numberOfComponents][numberOfAttributes];
		double[] tau = new double[numberOfComponents];

		for (int component = 0; component < numberOfComponents; component++) {
			for (int i = 0; i < statistics[component]._means.length; i++) {
				mu[component][i] = statistics[component]._means[i];
				sigma[component][i] = statistics[component]._m2[i] / statistics[component]._weightSum;
			}

			tau[component] = statistics[component]._weightSum / statistics[component]._n;
		}

		return new MixtureModel(mu, sigma, tau);
	}
}
