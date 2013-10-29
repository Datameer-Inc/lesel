package datameer.lesel.generative;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MomentStatisticsOutputFormat<K, V> extends TextOutputFormat<K, V> {

	private MomentStatisticsOutputCommitter _committer = null;

	public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
		if (_committer == null) {
			Path output = getOutputPath(context);
			_committer = new MomentStatisticsOutputCommitter(output, context);
		}
		return _committer;
	}

}
