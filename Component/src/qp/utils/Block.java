package qp.utils;

import java.io.Serializable;
import java.util.Vector;

public class Block implements Serializable {
	private static final long serialVersionUID = 1L;
	
	int MAX_SIZE;// Number of tuples per block
    static int BlockSize;  /* Number of bytes per block**/
    
    Vector<Tuple> tuples; // The tuples in the block
    Vector<Batch> batches;
    
    public Block(int numTuples, int blockSize) {
        MAX_SIZE = numTuples;
        BlockSize = blockSize;
        batches = new Vector<>(MAX_SIZE);
        tuples = new Vector<>(MAX_SIZE * blockSize);
    }

	public void clear() {
		tuples.clear();
		batches.clear();
	}
	
    public void addBatch(Batch b) {
        if(!isFull()) {
            batches.add(b);
            for (int i = 0; i < b.size(); ++i) {
                tuples.add(b.elementAt(i));
            }
        }
    }
    
    public Vector<Tuple> getTuples() {
        return tuples;
    }
    
    public Tuple getTuple(int i) {
        return (Tuple) tuples.elementAt(i);
    }
    
    public int getBlockSize() {
        return BlockSize;
    }
    
    public int getBatchSize() {
        return batches.size();
    }
    
    public int getTupleSize() {
        return tuples.size();
    }
    
    public boolean isEmpty() {
        return batches.isEmpty();
    }
    
    public boolean isFull() {
    		if(batches.size() >= MAX_SIZE) return true;
    		else return false;
    }
}