
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
//		Job job = Job.getInstance(conf, "SecSortDriver");
//		job.setJarByClass(Driver.class);
//		job.setMapperClass(PreProcessMapper.class);
//		job.setGroupingComparatorClass(MyGroupComparator.class);
//		job.setReducerClass(PreProcessReducer.class);
//		job.setPartitionerClass(MyPartioner.class);
//		job.setNumReduceTasks(5);
//		//Set MapOutput to MyKey.class
//		job.setMapOutputKeyClass(MyKey.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(NullWritable.class);
//		for (int i = 0; i < otherArgs.length - 1; ++i) {
//			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//		}
//		FileOutputFormat.setOutputPath(job,
//				new Path(otherArgs[otherArgs.length - 1]+"/preProcess"));
//
//		job.waitForCompletion(true);
		// job2
		Configuration conf1 = new Configuration();
		conf1.set("mapreduce.map.java.opts", "-Xmx5632m");
		conf1.set("mapreduce.map.memory.mb", "6144");
		Job training = Job.getInstance(conf1, "SecSortDriver");
		training.setJarByClass(Driver.class);
		training.setMapperClass(TrainingMapper.class);
		//Set MapOutput to MyKey.class
		training.setMapOutputKeyClass(NullWritable.class);
		training.setOutputKeyClass(NullWritable.class);
		training.setOutputValueClass(ClassifierWrapper.class);
		training.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(training,
				new Path
				(otherArgs[otherArgs.length - 1] +"/Models"));
		training.setNumReduceTasks(0);
		training.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(training, new
				Path("s3://amardeep-mr/methods.txt"));
//		NLineInputFormat.addInputPath(training, new
//				Path("methods.txt"));
		training.getConfiguration().setInt("mapreduce.input.lineinputformat"
				+ ".linespermap", 1);

		Path distributedFile = new Path
				(otherArgs[otherArgs.length - 1] + "/preProcess/part-r-00000");
		Path distributedFile1 = new Path(otherArgs[otherArgs.length - 1] + "/preProcess/part-r-00001");

		DistributedCache.addCacheFile(distributedFile.toUri(),training.getConfiguration());
		DistributedCache.addCacheFile(distributedFile1.toUri(),training.getConfiguration());
		DistributedCache.addLocalFiles(training.getConfiguration(), distributedFile.toString());
		DistributedCache.addLocalFiles(training.getConfiguration(), distributedFile1.toString());
		Path distributedFile2 = new Path
				(otherArgs[otherArgs.length - 1] + "/preProcess/part-r-00002");
		Path distributedFile3 = new Path(otherArgs[otherArgs.length - 1] + "/preProcess/part-r-00003");

		DistributedCache.addCacheFile(distributedFile2.toUri(),training.getConfiguration());
		DistributedCache.addCacheFile(distributedFile3.toUri(),training.getConfiguration());
		DistributedCache.addLocalFiles(training.getConfiguration(), distributedFile2.toString());
		DistributedCache.addLocalFiles(training.getConfiguration(), distributedFile3.toString());
		
		Path distributedFile4 = new Path(otherArgs[otherArgs.length - 1] + "/preProcess/part-r-00004");

		DistributedCache.addCacheFile(distributedFile4.toUri(),training.getConfiguration());
		DistributedCache.addLocalFiles(training.getConfiguration(), distributedFile4.toString());

		training.waitForCompletion(true);
//		Job predict = Job.getInstance(conf, "Predictor");
//		predict.setJarByClass(Driver.class);
//		predict.setMapperClass(Predictor.PredictorMapper.class);
//		predict.setReducerClass(Predictor.PredictorReducer.class);
//		//predict.setNumReduceTasks(1);
//		predict.setMapOutputKeyClass(IntWritable.class);
//		predict.setMapOutputValueClass(Text.class);
//		predict.setOutputKeyClass(NullWritable.class);
//		predict.setOutputValueClass(Text.class);
//		predict.setInputFormatClass(SequenceFileInputFormat.class);
//
//		predict.addCacheFile(new Path("s3://amardeep-mr/unlab/unlabeled.csv.bz2").toUri());
//		//predict.addCacheFile(new Path("/tmp/unlabeled.csv.bz2").toUri());
//		FileInputFormat.addInputPath(predict, new Path(otherArgs[otherArgs.length - 1] + "/Models"));
//		FileOutputFormat.setOutputPath(predict, new Path(otherArgs[otherArgs.length - 1] + "/Predictions"));
//
//		System.exit(predict.waitForCompletion(true) ? 0 : 1);
	}
}
