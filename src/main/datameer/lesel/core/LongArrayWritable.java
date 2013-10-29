package datameer.lesel.core;

import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {

	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(List<Long> list) {
		super(LongWritable.class, toWritableArray(list.toArray(new Long[list.size()])));
	}

	public LongArrayWritable(LongWritable[] array) {
		super(LongWritable.class, array);
	}

	public LongArrayWritable(long[] array) {
		super(DoubleWritable.class, toWritableArray(array));
	}

	private static LongWritable[] toWritableArray(Long[] array) {
		LongWritable[] result = new LongWritable[array.length];
		for (int i = 0; i < array.length; i++) {
			result[i] = new LongWritable(array[i]);
		}
		return result;
	}

	private static LongWritable[] toWritableArray(long[] array) {
		LongWritable[] result = new LongWritable[array.length];
		for (int i = 0; i < array.length; i++) {
			result[i] = new LongWritable(array[i]);
		}
		return result;
	}

	public long[] getAsNativeArray() {
		long[] result = new long[get().length];
		for (int i = 0; i < result.length; i++) {
			result[i] = ((LongWritable) get()[i]).get();
		}

		return result;
	}

}