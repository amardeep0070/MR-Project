package PreProcess;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//CompositeKey "MyKey" is used as output from the Map and input to the Reduce.
public class MyKey implements WritableComparable<MyKey>{
  //Key contains a tupple of StationId and Year.
  private String splitNumber;
  private String head;

  public MyKey(){
    //super();
  }
  public MyKey(String staionId, String year){
    this.splitNumber=staionId;
    this.head=year;
  }
  public void write(DataOutput out) throws IOException {
    out.writeUTF(splitNumber);
    out.writeUTF(head);
  }

  public void readFields(DataInput in) throws IOException {
    this.splitNumber=in.readUTF();
    this.head=in.readUTF();
  }

  //compare both stationID and year.
  public int compareTo(MyKey o) {
    int objectComapre=this.splitNumber.compareTo(o.getSplitNumber());
    if(objectComapre!=0){
      return objectComapre;
    }
    return this.head.compareTo(o.head);
  }
  //getters and Setters
  public String getSplitNumber() {
    return splitNumber;
  }
  public void setSplitNumber(String stationId) {
    this.splitNumber = stationId;
  }
  public String getHead() {
    return head;
  }
  public void setHead(String year) {
    this.head = year;
  }

}


