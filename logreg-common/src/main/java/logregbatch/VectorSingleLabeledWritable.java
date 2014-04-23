package logregbatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * For transfer of a vector and multiple numeric label.
 * Usefull for labeled data (supervised learning)
 * where we have to transmit not only the VectorWritable, but also the label 
 * Uses a VectorWritable instance internally
 * 
 * TODO Performance This is not very efficient, since it is wrapping around VectorWritables
 * Better to write a VectorPairWritable
 */
public class VectorSingleLabeledWritable implements Writable
{
  private VectorWritable vector;
  private IntWritable label;
  
  public VectorSingleLabeledWritable() {
    this.vector = new VectorWritable();
    this.label = new IntWritable();
  }

  public VectorSingleLabeledWritable(VectorWritable vector, IntWritable labels) {
    this.vector = vector;
    this.label = labels;
  }
  
  public void readFields(DataInput in) throws IOException {
    vector.readFields(in);
    label.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    vector.write(out);
    label.write(out);
  }
  
  @Override
  public int hashCode() {
    return vector.hashCode();
  }
  
  public int getLabels() {
    return label.get();
  }
  
  public Vector getVector() {
    return vector.get();
  }
  
  public void setLabels(int label) {
    this.label.set(label);
  }
  
  public void setVector(Vector vector) {
    this.vector.set(vector);
  }
}
