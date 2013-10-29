package datameer.lesel.s3vm;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import datameer.lesel.core.Instance;

public class ComputeMeansMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private static final Logger LOG = Logger.getLogger(ComputeMeansMapper.class);

	private InstanceMeans _means;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		Instance instance = Instance.createFromText(line);

		if (_means == null) {
			_means = InstanceMeans.create(instance.getX().length);
		}
		_means.updateMeans(instance);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		Gson gson = new Gson();
		String meansAsJson = gson.toJson(_means);

		LOG.info("Writing means for " + _means.getNumberOfInstances() + " input instances.");
		context.write(NullWritable.get(), new Text(meansAsJson));
	}
}
