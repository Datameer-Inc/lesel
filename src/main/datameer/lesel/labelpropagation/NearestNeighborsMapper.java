package datameer.lesel.labelpropagation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class NearestNeighborsMapper extends Mapper<NullWritable, InstanceWithNeighborsWritable, LongWritable, InstanceWithNeighborsWritable> {

	public void map(NullWritable key, InstanceWithNeighborsWritable value, Context context) throws IOException, InterruptedException {
		context.write(new LongWritable(value.getId()), value);
	}

}