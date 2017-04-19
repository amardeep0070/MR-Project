package PreProcess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;
import weka.filters.unsupervised.attribute.Remove;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.FastVector;
import weka.core.Instances;
import weka.classifiers.Evaluation;
import weka.classifiers.evaluation.NominalPrediction;
import weka.classifiers.lazy.IBk;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;

// retrieve info about method and corresponding input dataset from
// distributed file cache, and use that method to train the model
//
class TrainingMapper extends Mapper<LongWritable, Text, NullWritable, ClassifierWrapper> {

	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException {

		String[] temp = value.toString().split(" ");
		ArffSaver saver = new ArffSaver();
		ArffSaver saverTest = new ArffSaver();

		try {
			Configuration conf = context.getConfiguration();
			Path files[] = DistributedCache.getLocalCacheFiles(conf);
			File trainFile = new File(temp[1]);
			//File testFile= new File("part-r-00005");
			if (trainFile == null) {
				throw new RuntimeException(
						"No File In DistributedCache");
			}
			//Loading for Test File
//			CSVLoader loaderTest = new CSVLoader();
//			loaderTest.setSource(testFile);
//			Instances dataTest = loaderTest.getDataSet();
//			saverTest.setInstances(dataTest);
//			NumericToNominal convertTest = new NumericToNominal();
//			String[] optionsTest = new String[2];
//			optionsTest[0] = "-R";
//			optionsTest[1] = "21"; // range of variables to make numeric
//
//			convertTest.setOptions(optionsTest);
//			convertTest.setInputFormat(dataTest);
//			Instances arffDataSetTest;
//			arffDataSetTest = Filter.useFilter(dataTest, convertTest);
//			saverTest.setInstances(arffDataSetTest);
//			arffDataSetTest.setClassIndex(arffDataSetTest.numAttributes() - 1);
			//loading for trainFile
			CSVLoader loader = new CSVLoader();
			loader.setSource(trainFile);
			Instances data = loader.getDataSet();

			// save ARFF
			saver.setInstances(data);
			NumericToNominal convert = new NumericToNominal();
			String[] options = new String[2];
			options[0] = "-R";
			options[1] = "21"; // range of variables to make numeric

			convert.setOptions(options);
			convert.setInputFormat(data);
			Instances arffDataSet;
			arffDataSet = Filter.useFilter(data, convert);
			saver.setInstances(arffDataSet);
			//Training the Model
			arffDataSet.setClassIndex(arffDataSet.numAttributes() - 1);
			String classifierToUse = temp[0];
			ClassifierWrapper classifierUsed = new ClassifierWrapper();
			if (classifierToUse.equals("IBK")) {
				classifierUsed.setClassifier(new IBk(Integer.parseInt(temp[2])));
			}
			if (classifierToUse.equals("J48")) {
				classifierUsed.setClassifier(new J48());
			}
//			classifierUsed.getClassifier().buildClassifier(arffDataSet);
//			Evaluation eval = new Evaluation(arffDataSet);
//			eval.evaluateModel(classifierUsed.getClassifier(),arffDataSetTest);
//			System.out.println(eval.toSummaryString("\nResults\n======\n", false));
					int seed = 1;
					int folds = 10;
					FastVector predictions = new FastVector();
					Random rand = new Random(seed);
					Instances randData = new Instances(arffDataSet);
					randData.randomize(rand);
					//classifierUsed.getClassifier().buildClassifier(randData);
					for (int i = 0; i < 1; i++) {
						Evaluation eval = new Evaluation(randData);
						Instances train = randData.trainCV(folds, i);
						Instances test = randData.testCV(folds, i);
						classifierUsed.getClassifier().buildClassifier(train);
						eval.evaluateModel(classifierUsed.getClassifier(), test);
						predictions.appendElements(eval.predictions());
					}

					double accuracy = calculateAccuracy(predictions);
					System.out.println("Accuracy of the model" + classifierUsed.getClassifier().getClass().toString()
							+ "trained is :" + accuracy);

			context.write(NullWritable.get(), classifierUsed);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	  public static double calculateAccuracy(FastVector predictions) {
			double correct = 0;
	
			for (int i = 0; i < predictions.size(); i++) {
				NominalPrediction np = (NominalPrediction) predictions.elementAt(i);
				if (np.predicted() == np.actual()) {
					correct++;
				}
			}
	
			return 100 * correct / predictions.size();
		}
}
