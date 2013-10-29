package datameer.lesel.s3vm;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import datameer.lesel.core.Instance;

public class SgdReducer extends Reducer<DoubleWritable, InstanceWritable, NullWritable, Text> {
	public static String LEARNING_RATE = "learningRate";
	public static String REGULARIZER_CONSTANT = "regularizer";
	public static String UNLABELED_WEIGHT = "unlabeledWeight";
	public static String BIAS = "bias";

	private Context _context;

	private double _eta0;
	private double _lambda;
	private double _unlabeledWeight;
	private double _b;

	private int _t = 0;
	private double[] _w;

	protected void setup(Context context) throws IOException, InterruptedException {
		_context = context;

		_eta0 = context.getConfiguration().getFloat(LEARNING_RATE, 10.0F);
		_lambda = context.getConfiguration().getFloat(REGULARIZER_CONSTANT, 1.0F);
		_unlabeledWeight = context.getConfiguration().getFloat(UNLABELED_WEIGHT, 1.0F);
		_b = context.getConfiguration().getFloat(BIAS, 0.0F);
	}

	protected void reduce(DoubleWritable key, Iterable<InstanceWritable> values, Context context) throws IOException, InterruptedException {

		for (InstanceWritable instanceWritable : values) {
			Instance instance = instanceWritable.getInstance();

			if (_w == null) {
				_w = new double[instance.getX().length];
			}

			double eta = _eta0 / (1.0 + _lambda * _eta0 * _t);

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
					_w[i] += eta * _unlabeledWeight * 6.0 * Math.exp(-3.0 * (margin + _b) * (margin + _b)) * (margin + _b) * x[i];
				}
			}

			for (int i = 0; i < x.length; i++) {
				_w[i] -= eta * _lambda * _w[i];
			}

			_t++;
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		String result = "w: [";
		for (int i = 0; i < _w.length; i++) {
			result += _w[i] + ((i < _w.length - 1) ? "," : "");
		}
		result += "], b: " + _b;

		_context.write(NullWritable.get(), new Text(result));
	}
}
