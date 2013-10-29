package datameer.lesel.s3vm;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import datameer.lesel.core.Instance;

public class NormalizeMapper extends Mapper<LongWritable, Text, NullWritable, InstanceWritable> {
	public static String MEANS_PATH = "meansPath";

	private InstanceMeans _means = null;

	public NormalizeMapper() {
	}

	// for testing
	public NormalizeMapper(InstanceMeans means) {
		_means = means;
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		String meansPath = context.getConfiguration().get(MEANS_PATH);
		_means = readMeansFromJson(context, meansPath);
	}

	private InstanceMeans readMeansFromJson(Context context, String meansPathString) throws IOException {
		Path meansPath = new Path(meansPathString, MeansOutputCommitter.OVERALL_MEANS_FILENAME);
		FileSystem fileSystem = meansPath.getFileSystem(context.getConfiguration());

		InstanceMeans means = InstanceMeans.readMeansFile(meansPath, fileSystem);

		return means;
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		Instance instance = Instance.createFromText(line);

		double[] x = instance.getX();
		for (int i = 0; i < x.length; i++) {
			x[i] = x[i] - _means.getUnlabeledInstancesMeans()[i];
		}

		Instance normalizedInstance = Instance.create(x, instance.getY());
		context.write(NullWritable.get(), new InstanceWritable(normalizedInstance));
	}

}
