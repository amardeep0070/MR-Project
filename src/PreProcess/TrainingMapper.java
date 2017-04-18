package PreProcess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;


import weka.filters.unsupervised.attribute.Remove;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.Instances;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;

// retrieve info about method and corresponding input dataset from
// distributed file cache, and use that method to train the model
//
class TrainingMapper extends Mapper<LongWritable, Text, MyKey, Text> {

  public void map(LongWritable key, Text value, Context context) throws
                                                                 IOException, InterruptedException {

    String[] temp = value.toString().split(" ");
    ArffSaver saver = new ArffSaver();

    try {
      Configuration conf = context.getConfiguration();
      Path files[] = DistributedCache.getLocalCacheFiles(conf);
      File myFile = new File(temp[1]);
      if (myFile == null) {
        throw new RuntimeException(
            "No File In DistributedCache");
      }

      CSVLoader loader = new CSVLoader();
      loader.setSource(myFile);
      Instances data = loader.getDataSet();

      // save ARFF
      saver.setInstances(data);
      saver.setFile(new File("output"));
      saver.setDestination(new File("output"));
      saver.writeBatch();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // data
    Instances structure = saver.getInstances();
    // filter
    Remove rm = new Remove();
//    rm.setAttributeIndices("1");  // remove 1st attribute
    // classifier
    J48 j48 = new J48();
    j48.setUnpruned(true);        // using an unpruned J48
    // meta-classifier
    FilteredClassifier fc = new FilteredClassifier();
//    fc.setFilter(rm);
    fc.setClassifier(j48);
    // train and make predictions
    try {
      fc.buildClassifier(structure);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(structure.toSummaryString());
  }
}
