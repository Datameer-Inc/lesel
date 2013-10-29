package datameer.lesel.core;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.Progress;

@SuppressWarnings("rawtypes")
public class ReducerTestBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	protected FileSystem _testFileSystem;
	protected Class<KEYIN> _keyInClass;
	protected Class<VALUEIN> _valueInClass;

	public ReducerTestBase(Class<KEYIN> keyinClass, Class<VALUEIN> valueInClass) {
		_keyInClass = keyinClass;
		_valueInClass = valueInClass;
	}

	protected void setupFileSystem() throws IOException, URISyntaxException {
		_testFileSystem = mock(FileSystem.class);

		FileSystem.addFileSystemForTesting(new URI("test://test/"), new Configuration(), _testFileSystem);
	}

	protected Context createContext(Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer, final List<VALUEOUT> outputValues) throws IOException,
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

		RawKeyValueIterator keyValueIterator = new RawKeyValueIterator() {

			@Override
			public DataInputBuffer getKey() throws IOException {
				return null;
			}

			@Override
			public DataInputBuffer getValue() throws IOException {
				return null;
			}

			@Override
			public boolean next() throws IOException {
				return false;
			}

			@Override
			public void close() throws IOException {
			}

			@Override
			public Progress getProgress() {
				return null;
			}
		};

		Configuration configuration = new Configuration();
		TaskAttemptID taskAttemptId = new TaskAttemptID();

		Path outputPath = new Path("test://test/reducerOutput");
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, new TaskAttemptContext(configuration, taskAttemptId));

		Context context = reducer.new Context(configuration, taskAttemptId, keyValueIterator, null, null, recordWriter, committer, null, null, _keyInClass,
				_valueInClass);
		return context;
	}
}
