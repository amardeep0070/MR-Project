package PreProcess;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Writable;

import weka.classifiers.Classifier;

public class ClassifierWrapper implements Writable{
	Classifier classifier;

	
	public ClassifierWrapper() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ClassifierWrapper(Classifier classifier) {
		super();
		this.classifier = classifier;
	}

	public Classifier getClassifier() {
		return classifier;
	}

	public void setClassifier(Classifier classifier) {
		this.classifier = classifier;
	}

	public void readFields(DataInput arg0) throws IOException {
		int numBytes = arg0.readInt();
		byte data[] = new byte[numBytes];	
		arg0.readFully(data);
		ByteArrayInputStream in = new ByteArrayInputStream(data);
	    ObjectInputStream is = new ObjectInputStream(in);
	    try {
			classifier = (Classifier) is.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException();
		}
	}

	public void write(DataOutput arg0) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = new ObjectOutputStream(out);
	    os.writeObject(classifier);
	    arg0.writeInt(out.toByteArray().length);
		arg0.write(out.toByteArray());		
	}

}
