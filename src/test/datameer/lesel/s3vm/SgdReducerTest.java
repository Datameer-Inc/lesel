package datameer.lesel.s3vm;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import datameer.lesel.core.Instance;
import datameer.lesel.core.ReducerTestBase;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SgdReducerTest extends ReducerTestBase<DoubleWritable, InstanceWritable, NullWritable, Text> {
	private FSDataOutputStream _outputStream;

	public SgdReducerTest() {
		super(DoubleWritable.class, InstanceWritable.class);
	}

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();

		_outputStream = mock(FSDataOutputStream.class);
		when(_testFileSystem.create(any(Path.class))).thenReturn(_outputStream);
	}

	@Test
	public void testSgdReducer() throws IOException, InterruptedException {
		SgdReducer reducer = new SgdReducer();
		List<Text> outputValues = Lists.newArrayList();
		Context context = createContext(reducer, outputValues);
		context.getConfiguration().setFloat(SgdReducer.LEARNING_RATE, 10.0F);
		context.getConfiguration().setFloat(SgdReducer.REGULARIZER_CONSTANT, 1.0F);
		context.getConfiguration().setFloat(SgdReducer.BIAS, 0.0F);

		reducer.setup(context);

		reducer.reduce(new DoubleWritable(0.1), ImmutableList.of(new InstanceWritable(Instance.createFromText("1.0, 2.0, 3.0, 1.0"))), context);
		reducer.reduce(new DoubleWritable(0.2), ImmutableList.of(new InstanceWritable(Instance.createFromText("3.0, 2.0, 1.0, -1.0"))), context);
		reducer.reduce(new DoubleWritable(0.3), ImmutableList.of(new InstanceWritable(Instance.createFromText("-1.0, -2.0, -3.0, 0.0"))), context);
		reducer.reduce(new DoubleWritable(0.4), ImmutableList.of(new InstanceWritable(Instance.createFromText("1.0, 2.0, 3.0, 1.0"))), context);
		reducer.reduce(new DoubleWritable(0.5), ImmutableList.of(new InstanceWritable(Instance.createFromText("0.0, -2.0, 1.0, 0.0"))), context);

		reducer.cleanup(context);

		assertThat(outputValues).hasSize(1);
		assertThat(outputValues.get(0).toString()).isEqualTo("w: [2.2616407982261664,4.434589800458036,6.607538802653469], b: 0.0");
	}
}
