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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import datameer.lesel.core.MapperTestBase;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class BlockNeighborsMapperTest extends MapperTestBase<LongWritable, Text, LongWritable, BlockInstanceWritable> {
	private FSDataOutputStream _outputStream;

	@Before
	public void setup() throws IOException, URISyntaxException {
		setupFileSystem();

		_outputStream = mock(FSDataOutputStream.class);
		when(_testFileSystem.create(any(Path.class))).thenReturn(_outputStream);
	}

	@Test
	public void testBlockNeighborsMapper() throws IOException, InterruptedException {
		BlockNeighborsMapper mapper = new BlockNeighborsMapper();
		List<BlockInstanceWritable> outputValues = Lists.newArrayList();
		Context context = createContext(mapper, outputValues);

		mapper.map(new LongWritable(3), new Text("1.0, 2.0, 3.0, 0.0"), context);
		mapper.map(new LongWritable(3), new Text("3.0, 2.0, 1.0, 0.0"), context);

		assertThat(outputValues).hasSize(16);
	}
}