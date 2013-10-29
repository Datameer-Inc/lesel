package datameer.lesel.labelpropagation;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.fest.assertions.Delta;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import datameer.lesel.core.Instance;
import datameer.lesel.core.ReducerTestBase;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class BlockNeighborsReducerTest extends ReducerTestBase<LongWritable, BlockInstanceWritable, NullWritable, InstanceWithNeighborsWritable> {
	private FSDataOutputStream _outputStream;

	public BlockNeighborsReducerTest() {
		super(LongWritable.class, BlockInstanceWritable.class);
	}

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();

		_outputStream = mock(FSDataOutputStream.class);
		when(_testFileSystem.create(any(Path.class))).thenReturn(_outputStream);
	}

	@Test
	public void testBlockNeighborsReducer() throws IOException, InterruptedException {
		BlockNeighborsReducer reducer = new BlockNeighborsReducer();
		List<InstanceWithNeighborsWritable> outputValues = Lists.newArrayList();
		Context context = createContext(reducer, outputValues);

		ImmutableList.Builder<BlockInstanceWritable> listBuilder = ImmutableList.builder();
		listBuilder.add(new BlockInstanceWritable(0, Instance.createFromText("1.0, 2.0, 3.0, 1.0"), BlockInstanceWritable.NeighborStatus.IS_INSTANCE));
		listBuilder.add(new BlockInstanceWritable(1, Instance.createFromText("1.0, 3.0, 3.0, 1.0"), BlockInstanceWritable.NeighborStatus.IS_NEIGHBOR));
		listBuilder.add(new BlockInstanceWritable(2, Instance.createFromText("1.0, 4.0, 3.0, 1.0"), BlockInstanceWritable.NeighborStatus.IS_NEIGHBOR));
		listBuilder.add(new BlockInstanceWritable(3, Instance.createFromText("1.0, 5.0, 3.0, 1.0"), BlockInstanceWritable.NeighborStatus.IS_NEIGHBOR));
		listBuilder.add(new BlockInstanceWritable(4, Instance.createFromText("1.0, 6.0, 3.0, 1.0"), BlockInstanceWritable.NeighborStatus.IS_NEIGHBOR));
		listBuilder.add(new BlockInstanceWritable(5, Instance.createFromText("1.0, 7.0, 3.0, 1.0"), BlockInstanceWritable.NeighborStatus.IS_NEIGHBOR));

		reducer.reduce(new LongWritable(0), listBuilder.build(), context);

		assertThat(outputValues).hasSize(1);
		InstanceWithNeighborsWritable instance = outputValues.get(0);
		assertThat(instance.getId()).isEqualTo(0);
		assertThat(instance.getInstance().getX()).isEqualTo(new double[] { 1.0, 2.0, 3.0 }, Delta.delta(1e-6));
		assertThat(instance.getInstance().getY()).isEqualTo(1.0, Delta.delta(1e-6));
		assertThat(instance.getLabel()).isEqualTo(1.0);
		assertThat(instance.getDistances()).containsOnly(1.0, 16.0, 9.0, 4.0);
		assertThat(instance.getNeighborIds()).containsOnly(1L, 2L, 3L, 4L);
	}
}