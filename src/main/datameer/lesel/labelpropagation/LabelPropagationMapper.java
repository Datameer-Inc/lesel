package datameer.lesel.labelpropagation;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class LabelPropagationMapper extends Mapper<NullWritable, InstanceWithNeighborsWritable, LongWritable, LabelPropagationWritable> {

	public void map(NullWritable key, InstanceWithNeighborsWritable value, Context context) throws IOException, InterruptedException {
		List<Long> neighborIds = value.getNeighborIds();

		for (Long id : neighborIds) {
			context.write(new LongWritable(id), new LabelPropagationWritable(value.getLabel()));
		}

		context.write(new LongWritable(value.getId()), new LabelPropagationWritable(value));

	}
}
