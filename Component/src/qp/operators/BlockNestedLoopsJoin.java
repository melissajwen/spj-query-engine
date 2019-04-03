package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Block;
import qp.utils.Tuple;

public class BlockNestedLoopsJoin extends Join {
	
	/**************** Class Members ****************/   
	
	// The following fields are taken from the NestedJoin class
	int batchsize;  //Number of tuples per out batch
	
	int leftindex;     // Index of the join attribute in left tuple
	int rightindex;    // Index of the join attribute in right tuple

	String rfname;    // The file name where the right table is materialized
	
	static int filenum = 0;   // To get unique filenum for this operation
	
	Batch outbatch;   // Output buffer
	Batch leftbatch;  // Buffer for left input stream
	Block leftblock;      // Block for left input stream
	
	Batch rightbatch;  // Buffer for right input stream
	ObjectInputStream in; // File pointer to the right hand materialized file
	
	int lcurs;    // Cursor for left side buffer
	int rcurs;    // Cursor for right side buffer
	boolean eosl;  // Whether end of stream (left table) is reached
	boolean eosr;  // End of stream (right table)
	
	// This field is unique to the BlockNestedLoopsJoin class
	int blocksize;  // number of batches per block
	
	
	/**************** Methods ****************/ 
	
	// Constructor
	public BlockNestedLoopsJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}   
	
	// Open the operator, return true if successful
	 public boolean open() {
		// Initialize necessary parameters
		batchsize = Batch.getPageSize() / schema.getTupleSize();

		Attribute leftattr = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		
		// Find indices of join attributes
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);
		
		// Temporary batch to hold pages from right to materialize into file
		Batch rightpage;
	
		// Initialize cursors of input buffers
		lcurs = 0;
		rcurs = 0;
		eosl = false; // Right stream needs to be repeatedly scanned, so this must be set to false until end is reached
		eosr = true; // RHS table materialized for nested join
		
		if (!right.open()) {
			return false;
		} 
		else { 
			filenum++;
			rfname = "Temp" + String.valueOf(filenum) + "_BlockNestedLoopsJoin";
			
			// Materialize the RHS into a file
			try {
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
				while ((rightpage = right.next()) != null) {
					out.writeObject(rightpage);
				}
				out.close();
			} 
			catch (IOException io) {
				System.out.println("BlockNestedLoopsJoin: writing the temporary file error");
				return false;
			}
			if (!right.close()) {
				return false;
			}
		}
		
		if (left.open()) {
			return true;
		}
		else {
			return false;
		}
	}
	
	 // From input buffers, select tuples satisfying join condition and return a page of output tuples
	 public Batch next() {
		//System.out.print("BlockNestedLoopsJoin:--------------------------in next----------------");
		//Debug.PPrint(con);
		//System.out.println();
		 
		int i, j;
		
		// If the cursor is not at the start, return
		if (eosl) {
			close();
			return null;
		}
		
		// Page to hold matching tuples
		outbatch = new Batch(batchsize);
		
		while (!outbatch.isFull()) {
			if (lcurs == 0 && eosr == true) {
				// Create and populate a new block containing pages from left
				leftblock = new Block(blocksize, batchsize);
				
				// Write batches from left into leftblock
				while (!eosl && !leftblock.isFull()) {
					leftbatch = (Batch) left.next();
					if (leftbatch != null) {
						leftblock.addBatch(leftbatch);
					} 
					else {
						break;
					}
				}
				
				// If nothing was read in, terminate
				if (leftblock.isEmpty()) {
					eosl = true;
					return outbatch;
				}
				
				// Left block isn't empty, so we need to start scanning the right table
				try {                   
					in = new ObjectInputStream(new FileInputStream(rfname));
					eosr = false;
				} 
				catch (IOException io) {
					System.err.println("BlockNestedLoopsJoin: Error in reading file");
					System.exit(1);
				}
			}
			
			// While the end of the right buffer hasn't been reached
			while (eosr == false) {
				try {
					// Retrieve the right batch from file
					if (rcurs == 0 && lcurs == 0) {
						rightbatch = (Batch) in.readObject();
					}
					
					// Iterate over all batches in left block
					for (i = lcurs; i < leftblock.getTupleSize(); ++i) {
						for (j = rcurs; j < rightbatch.size(); ++j) {
							// Retrieve the relevant tuples 
							Tuple lefttuple = leftblock.getTuple(i);
							Tuple righttuple = rightbatch.elementAt(j);
							
							// If the tuples match
							if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
								// Output the matching tuple
								Tuple outtuple = lefttuple.joinWith(righttuple);
								outbatch.add(outtuple);
								
								//Debug.PPrint(outtuple);
								//System.out.println();
								
								// If this addition fills up the current batch, need to modify the cursors and return
								if (outbatch.isFull()) {
									if (i == leftblock.getTupleSize() - 1 && j == rightbatch.size() - 1) { // case 1, both the left block and right batch are done
										lcurs = 0;
										rcurs = 0;
									} 
									else if (i != leftblock.getTupleSize() - 1 && j == rightbatch.size() - 1) { // case 2, right batch is done but still space in the left block
										lcurs = i + 1;
										rcurs = 0;
									} 
									else if (i == leftblock.getTupleSize() - 1 && j != rightbatch.size() - 1) { // case 3, left block is full
										lcurs = i;
										rcurs = j + 1;
									} 
									else { // move onto next tuple in right batch
										lcurs = i;
										rcurs = j + 1;
									}
									return outbatch;
								}
							}
						}
						rcurs = 0;
					}
					
					lcurs = 0;
				} 
				catch (EOFException e) {
					try {
						in.close();
					} 
					catch (IOException io) {
						System.out.println("BlockNestedLoopsJoin: Error in temporary file reading");
					}
					eosr = true;
				} 
				catch (ClassNotFoundException c) {
					System.out.println("BlockNestedLoopsJoin: Some error in deserialization ");
					System.exit(1);
				} 
				catch (IOException io) {
					System.out.println("BlockNestedLoopsJoin: temporary file reading error");
					System.exit(1);
				}
			}
		}
		return outbatch;
	}
	
	
	// Close the operator
	public boolean close() {
		// Delete the temporary file
		File f = new File(rfname);
		f.delete();
		return true;
	}
}