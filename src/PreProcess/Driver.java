package PreProcess;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.filecache.DistributedCache;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "SecSortDriver");
		job.setJarByClass(Driver.class);
		job.setMapperClass(PreProcessMapper.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);
		job.setReducerClass(PreProcessReducer.class);
		job.setPartitionerClass(MyPartioner.class);
		job.setNumReduceTasks(6);
		//Set MapOutput to MyKey.class
		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));

		job.waitForCompletion(true);
		// job2
		Configuration conf1 = new Configuration();
		Job training = Job.getInstance(conf1, "SecSortDriver");
		training.setJarByClass(Driver.class);
		training.setMapperClass(TrainingMapper.class);
		//Set MapOutput to MyKey.class
		training.setMapOutputKeyClass(NullWritable.class);
		training.setOutputKeyClass(NullWritable.class);
		training.setOutputValueClass(ClassifierWrapper.class);
		FileOutputFormat.setOutputPath(training,
				new Path
				("out1"));
		training.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(training, new
				Path("methods.txt"));
		training.getConfiguration().setInt("mapreduce.input.lineinputformat"
				+ ".linespermap", 1);

		Path distributedFile = new Path
				("/tmp/output/part-r-00000");
		Path distributedFile1 = new Path("/tmp/output/part-r-00005");

		DistributedCache.addCacheFile(distributedFile.toUri(),training.getConfiguration());
		    DistributedCache.addCacheFile(distributedFile1.toUri(),training.getConfiguration());
		DistributedCache.addLocalFiles(training.getConfiguration(), distributedFile.toString());
		    DistributedCache.addLocalFiles(training.getConfiguration(), distributedFile1.toString());

		System.exit(training.waitForCompletion(true) ? 0 : 1);
	}
}
