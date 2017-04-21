

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PreProcessMapper extends Mapper<LongWritable, Text, MyKey, Text> {
  public  int numberOfModels;
  public int samplingPer;
  int[] columnsOfInterest = {1, 2, 3, 5, 6, 7, 9, 10, 11, 12, 13, 14, 17, 955, 956, 960, 961, 962, 965, 976, 26};
  @Override
  public void setup(Context context){
    //get numberOfPages and delta from config
    //		Configuration conf = context.getConfiguration();
    //		numberOfPages = Long.parseLong(conf.get("numberOfPages"));
    //		delta=Long.parseLong(conf.get("delta"));
    //TODO get number of models from config
    numberOfModels=10;
    samplingPer=4;
  }
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Random random = new Random();
  // int randomNumber = random.nextInt(7);
    String[] columnContent=value.toString().split(",");
    String output="";
    if(value.toString().startsWith("SAMPLING_EVENT_ID")){
      for(int k:columnsOfInterest){
        output=output+","+columnContent[k].trim();
      }
      for(int i=0; i<=numberOfModels;i++){
        MyKey k = new MyKey(i+"",0+"");
        context.write(k, new Text(output.substring(1).trim()));
      }

    }
    else{
      output="";
      for(int k:columnsOfInterest){
        String temp=columnContent[k].trim();
        if(k==26 && !temp.equalsIgnoreCase("X")&& Double.parseDouble(columnContent[k].trim())>0 ){
          temp="1";
        }
        if(k==26 && temp.equalsIgnoreCase("X")){
        	temp="1";
        }
        output=output+","+temp;
      }
      for (int i = 0; i < numberOfModels; i++) {
    	  Random r = new Random();
//    	  if ((int) (Math.random() * 100) <= samplingPer) {
    	  if (r.nextInt(100) <= 20) {
				 MyKey k = new MyKey(i+"",1+"");
			      context.write(k, new Text(output.substring(1).trim()));
			}
		}
     
    }
  }

}
