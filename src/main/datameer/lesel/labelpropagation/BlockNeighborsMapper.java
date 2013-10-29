package datameer.lesel.labelpropagation;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Preconditions;

import datameer.lesel.core.Instance;

public class BlockNeighborsMapper extends Mapper<LongWritable, Text, LongWritable, BlockInstanceWritable> {
	public static int NUMBER_OF_BLOCKS = 4;
	public static long MAXIMUM_ID_SIZE = 10000;
	private Random _random = new Random(11);
	private long _idCounter;

	protected void setup(Context context) throws IOException, InterruptedException {
		_idCounter = 0;
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int blockNumber = _random.nextInt(NUMBER_OF_BLOCKS);
		Instance instance = Instance.createFromText(value.toString());
		long id = createId(context, blockNumber);

		BlockInstanceWritable neighborSideInstance = new BlockInstanceWritable(id, instance, BlockInstanceWritable.NeighborStatus.IS_NEIGHBOR);
		for (int i = 0; i < NUMBER_OF_BLOCKS; i++) {
			LongWritable blockPairId = new LongWritable(blockNumber + i * NUMBER_OF_BLOCKS);
			context.write(blockPairId, neighborSideInstance);
		}

		BlockInstanceWritable instanceSideInstance = new BlockInstanceWritable(id, instance, BlockInstanceWritable.NeighborStatus.IS_INSTANCE);
		for (int i = 0; i < NUMBER_OF_BLOCKS; i++) {
			LongWritable blockPairId = new LongWritable(i + blockNumber * NUMBER_OF_BLOCKS);
			context.write(blockPairId, instanceSideInstance);
		}
	}

	private long createId(Context context, int blockNumber) {
		long id = (long) context.getTaskAttemptID().getTaskID().getId() * MAXIMUM_ID_SIZE;
		id += _idCounter;
		_idCounter++;
		Preconditions.checkArgument(_idCounter < MAXIMUM_ID_SIZE, "Maximum id number exceeded.");

		return id;
	}
}
