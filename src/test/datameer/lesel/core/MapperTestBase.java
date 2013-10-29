package datameer.lesel.core;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

@SuppressWarnings("rawtypes")
public class MapperTestBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	protected FileSystem _testFileSystem;

	public MapperTestBase() {
	}

	protected void setupFileSystem() throws IOException, URISyntaxException {
		_testFileSystem = mock(FileSystem.class);

		FileSystem.addFileSystemForTesting(new URI("test://test/"), new Configuration(), _testFileSystem);
	}

	protected Context createContext(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper, final List<VALUEOUT> outputValues) throws IOException,
			InterruptedException {
		RecordWriter<KEYOUT, VALUEOUT> recordWriter = new RecordWriter<KEYOUT, VALUEOUT>() {
			public List<VALUEOUT> _values = outputValues;

			@Override
			public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {
				_values.add(value);
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException, InterruptedException {

			}

		};

		Path splitPath = new Path("test://test/mapperInput");
		Configuration configuration = new Configuration();
		TaskAttemptID taskAttemptId = new TaskAttemptID();
		InputSplit split = new FileSplit(splitPath, 0, 10000, new String[] {});

		Path outputPath = new Path("test://test/mapperOutput");
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, new TaskAttemptContext(configuration, taskAttemptId));

		Context context = mapper.new Context(configuration, taskAttemptId, null, recordWriter, committer, null, split) {
			FileSplit _split = new FileSplit(new Path("test://test/mapperOutput"), 0, 10000, new String[] {});

			public InputSplit getInputSplit() {
				return _split;
			}
		};
		return context;
	}
}
