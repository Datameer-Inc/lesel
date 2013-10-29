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
public class LabelPropagationReducerTest extends ReducerTestBase<LongWritable, LabelPropagationWritable, NullWritable, InstanceWithNeighborsWritable> {
	private FSDataOutputStream _outputStream;

	public LabelPropagationReducerTest() {
		super(LongWritable.class, LabelPropagationWritable.class);
	}

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();

		_outputStream = mock(FSDataOutputStream.class);
		when(_testFileSystem.create(any(Path.class))).thenReturn(_outputStream);
	}

	@Test
	public void testLabelPropagationReducer() throws IOException, InterruptedException {
		LabelPropagationReducer reducer = new LabelPropagationReducer();
		List<InstanceWithNeighborsWritable> outputValues = Lists.newArrayList();
		Context context = createContext(reducer, outputValues);

		ImmutableList.Builder<LabelPropagationWritable> listBuilder = ImmutableList.builder();
		listBuilder.add(new LabelPropagationWritable(new InstanceWithNeighborsWritable(Instance.createFromText("1.0, 2.0, 3.0, 1.0"), 0, ImmutableList.of(1.0,
				2.0, 3.0, 4.0), ImmutableList.of(1L, 2L, 3L, 4L), 1.0)));
		listBuilder.add(new LabelPropagationWritable(0.6));
		listBuilder.add(new LabelPropagationWritable(0.1));
		listBuilder.add(new LabelPropagationWritable(1.0));
		listBuilder.add(new LabelPropagationWritable(-0.6));

		reducer.reduce(new LongWritable(0), listBuilder.build(), context);

		assertThat(outputValues).hasSize(1);
		InstanceWithNeighborsWritable instance = outputValues.get(0);
		assertThat(instance.getId()).isEqualTo(0);
		assertThat(instance.getInstance().getX()).isEqualTo(new double[] { 1.0, 2.0, 3.0 }, Delta.delta(1e-6));
		assertThat(instance.getInstance().getY()).isEqualTo(1.0, Delta.delta(1e-6));
		assertThat(instance.getLabel()).isEqualTo(0.637, Delta.delta(1e-3));
		assertThat(instance.getDistances()).containsOnly(1.0, 2.0, 3.0, 4.0);
		assertThat(instance.getNeighborIds()).containsOnly(1L, 2L, 3L, 4L);
	}
}