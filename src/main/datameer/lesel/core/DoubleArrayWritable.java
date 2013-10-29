package datameer.lesel.core;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {

	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}

	public DoubleArrayWritable(List<Double> list) {
		super(DoubleWritable.class, toWritableArray(list.toArray(new Double[list.size()])));
	}

	public DoubleArrayWritable(DoubleWritable[] array) {
		super(DoubleWritable.class, array);
	}

	public DoubleArrayWritable(double[] array) {
		super(DoubleWritable.class, toWritableArray(array));
	}

	public DoubleArrayWritable(DoubleArrayWritable source) {
		super(DoubleWritable.class, Arrays.copyOf(source.get(), source.get().length));
	}

	private static DoubleWritable[] toWritableArray(Double[] array) {
		DoubleWritable[] result = new DoubleWritable[array.length];
		for (int i = 0; i < array.length; i++) {
			result[i] = new DoubleWritable(array[i]);
		}
		return result;
	}

	private static DoubleWritable[] toWritableArray(double[] array) {
		DoubleWritable[] result = new DoubleWritable[array.length];
		for (int i = 0; i < array.length; i++) {
			result[i] = new DoubleWritable(array[i]);
		}
		return result;
	}

	public double[] getAsNativeArray() {
		double[] result = new double[get().length];
		for (int i = 0; i < result.length; i++) {
			result[i] = ((DoubleWritable) get()[i]).get();
		}

		return result;
	}

}
