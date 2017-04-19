package PreProcess;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;

public class Predictor {

	public static class PredictorMapper extends Mapper<Object, ClassifierWrapper, IntWritable, Text> {

		Instances testDataSet; // Stores the instances of the unlabeled csv file
		int columnsOfInterest[] = {1, 2, 3, 5, 6, 7, 9, 10, 11, 12, 13, 14, 17, 955, 956, 960, 961, 962, 965, 976, 26};
		List<String> samplingEvents; // Stores all the sampling events
		List<String> filteredColumns; // Stores all the filtered columns

		private BufferedReader getBufferedReaderForCompressedFile(String fileIn)
				throws FileNotFoundException, CompressorException {
			/*
			 * Return the buffered reader object for compressed file
			 * i.e. specially for .bz2 files
			 */
			FileInputStream fin = new FileInputStream(fileIn);
			BufferedInputStream bis = new BufferedInputStream(fin);
			CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
			BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
			return br2;
		}

		private InputStream getTestDataSet(String fileName) throws IOException {
			BufferedReader cacheReader = null;
			try {
				cacheReader = getBufferedReaderForCompressedFile(fileName);
			} catch (CompressorException e) {

			}
			/*
			 * Read the individual lines of the csv file line by line
			 * and filter out the columns in which we are interested.
			 */
			String line = "";
			while ((line = cacheReader.readLine()) != null) {
				String cols[] = line.split(",");
				StringBuffer sb = new StringBuffer();
				samplingEvents.add(cols[0]);
				if (cols[0].equalsIgnoreCase("SAMPLING_EVENT_ID")) {
					for (int i = 0; i < columnsOfInterest.length - 1; i++) {
						sb.append(cols[columnsOfInterest[i]] + ",");
					}
					sb.append(cols[columnsOfInterest[columnsOfInterest.length - 1]]);
				} else {
					for (int i = 0; i < columnsOfInterest.length - 1; i++) {
						sb.append(cols[columnsOfInterest[i]] + ",");
					}
					if ((int) (Math.random() * 10) < 5)
						sb.append("0");
					else
						sb.append("1");
				}
				filteredColumns.add(sb.toString());
			}
			cacheReader.close();
			/*
			 * Convert the filtered lines read into an
			 * InputStream object so that it can be loaded 
			 * by CSVLoader object for creating an arff file.
			 */
			String data = StringUtils.join(filteredColumns, '\n');
			InputStream is = IOUtils.toInputStream(data, "UTF-8");
			return is;
		}

		private void readFromDistributedCache() throws Exception {

			//InputStream inputStream = getTestDataSet("input/PreProcessorInput/UnLabeled/unlabeled.csv.bz2");
			InputStream inputStream = getTestDataSet("./unlabeled.csv.bz2");
			CSVLoader csv = new CSVLoader();
			csv.setSource(inputStream);
			Instances dataSet = csv.getDataSet();

			ArffSaver save = new ArffSaver();
			save.setInstances(dataSet);

			/* Code to convert the numeric values for Agelaius_phoeniceus
			 * to nominal values i.e. discrete values {0,1}
			 */
			NumericToNominal convert = new NumericToNominal();
			String[] options = new String[2];
			options[0] = "-R";
			options[1] = "21";

			convert.setOptions(options);
			convert.setInputFormat(dataSet);

			testDataSet = Filter.useFilter(dataSet, convert);
			save.setInstances(testDataSet);

			/* Set the class index to Agelaius_phoeniceus */
			testDataSet.setClassIndex(testDataSet.numAttributes() - 1);
		}

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			samplingEvents = new ArrayList<String>();
			filteredColumns = new ArrayList<String>();
			try {
				readFromDistributedCache();
			} catch (Exception e) {
				throw new InterruptedException();
			}
			// Emit the header to be displayed in the final result file
			context.write(new IntWritable(0), new Text(samplingEvents.get(0) + "," + "SAW_AGELAIUS_PHOENICEUS"));
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(Object key, ClassifierWrapper value, Context context) throws IOException, InterruptedException {
			for (int i = 0; i < testDataSet.numInstances(); i++) {
				Instance newInstance = testDataSet.instance(i);

				try {
					/*
					 * Code to predict the output for each of the test instance
					 * present in the testInstances dataset.
					 */
					double predictedClass = value.getClassifier().classifyInstance(newInstance);
					String predictedValue = testDataSet.classAttribute().value((int) predictedClass);
					context.write(new IntWritable(i + 1), new Text(samplingEvents.get(i + 1) + "," + predictedValue));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static class PredictorReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double count = 0;
			double sum = 0;
			String eventId = "";
			/*
			 * Iterate thorugh each of the values which will be emitted by the 
			 * model in their respective Mappers and emit the averaged value
			 * into the output file alongwith the SAMPLING_EVENT_ID.
			 */
			if(key.get() != 0){
				for(Text val : values){
					if(count == 0)
						eventId = val.toString().split(",")[0];
					
					sum += Double.parseDouble(val.toString().split(",")[1]);
					count++;
				}
				context.write(NullWritable.get(), new Text(eventId + "," + (int) Math.round((sum / count))));
			}else{
				for(Text val : values){
					//To print out the header of the file
					context.write(NullWritable.get(), val);
					break;
				}
			}
		}
	}
}
