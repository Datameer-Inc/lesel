package datameer.lesel.labelpropagation;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConvertToTextMapper extends Mapper<NullWritable, InstanceWithNeighborsWritable, NullWritable, Text> {

	public void map(NullWritable key, InstanceWithNeighborsWritable value, Context context) throws IOException, InterruptedException {
		String result = value.getInstance().toString();
		result += ": " + Math.signum(value.getLabel());
		context.write(NullWritable.get(), new Text(result));
	}

}