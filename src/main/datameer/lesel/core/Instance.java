package datameer.lesel.core;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;

public class Instance {

	private double[] _x; // the instance
	private double _y; // the label

	private Instance(double[] x, double y) {
		_x = x;
		_y = y;
	}

	public double[] getX() {
		return _x;
	}

	public double getY() {
		return _y;
	}

	public boolean hasLabel() {
		return _y != 0.0;
	}

	public DoubleArrayWritable getAsArray() {
		DoubleWritable[] array = new DoubleWritable[_x.length + 1];

		for (int i = 0; i < _x.length; i++) {
			array[i] = new DoubleWritable(_x[i]);
		}
		array[_x.length] = new DoubleWritable(_y);

		return new DoubleArrayWritable(array);
	}

	public String toString() {
		StringBuilder resultBuilder = new StringBuilder();

		for (double value : _x) {
			resultBuilder.append(value).append(", ");
		}
		resultBuilder.append(_y);

		return resultBuilder.toString();
	}

	static public Instance copyOf(Instance instance) {
		return new Instance(Arrays.copyOf(instance.getX(), instance.getX().length), instance.getY());
	}

	static public Instance create(double[] x, double y) {
		return new Instance(x, y);
	}

	static public Instance createFromText(String line) {
		Scanner scanner = new Scanner(line);
		scanner.useDelimiter("\\s*,\\s*");
		scanner.useLocale(Locale.US);

		List<Double> values = Lists.newArrayList();

		while (scanner.hasNext()) {
			values.add(scanner.nextDouble());
		}

		int numberOfValues = values.size();
		double y = values.get(numberOfValues - 1);
		values.remove(numberOfValues - 1);

		double[] x = Doubles.toArray(values);

		return new Instance(x, y);
	}

	static public Instance createFromArray(ArrayWritable arrayWritable) {
		Writable[] array = arrayWritable.get();

		double y = ((DoubleWritable) array[array.length - 1]).get();
		double[] x = new double[array.length - 1];
		for (int i = 0; i < array.length - 1; i++) {
			x[i] = ((DoubleWritable) array[i]).get();
		}

		return new Instance(x, y);

	}

	public static double euclidianDistance(Instance instance1, Instance instance2) {
		double[] x1 = instance1.getX();
		double[] x2 = instance2.getX();

		double result = 0.0;
		for (int i = 0; i < x1.length; i++) {
			double diff = (x1[i] - x2[i]);
			result += diff * diff;
		}

		return result;
	}
}
