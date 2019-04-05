package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class HashJoin extends Join {

	/**************** Class Members ****************/   
	
	int batchsize;  //Number of tuples per out batch
	
	int leftindex;     // Index of the join attribute in left tuple
	int rightindex;    // Index of the join attribute in right tuple

	String rfname; // The file name where the right table is materialize

	static int filenum = 0; // To get unique filenum for this operation
	
	Batch outbatch;   // Output buffer
	Batch leftbatch;  // Buffer for left input stream
	
	Batch rightbatch;  // Buffer for right input stream
	ObjectInputStream in; // File pointer to the right hand materialized file
	
	int lcurs;    // Cursor for left side buffer
	int rcurs;    // Cursor for right side buffer
	int kcurs;	  // Cursor for the list of search keys
	
	boolean eosl;	// Whether left table has been completely partitioned 
	boolean eosr;	// Whether right table has been completely partitioned 
	boolean complete; 	// check if hashjoin has been completed;
	boolean built; 		// check if there is a need to partition again
	
	HashMap<Object, ArrayList<Tuple>> lefttable; // hashtable for left table
	HashMap<Object, ArrayList<Tuple>> righttable; // hashtable for right table
	HashMap<Object, ArrayList<Tuple>> probetable; // hashtable for probing
	
	List<Object> leftkeys; // list containing all the keys for left table;
	List<Object> rightkeys; // list containing all the keys for right table;


	/**************** Methods ****************/ 

	// Constructor
	public HashJoin(Join jn) {
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
			
	   	lefttable = new HashMap<Object, ArrayList<Tuple>>();
		righttable = new HashMap<Object, ArrayList<Tuple>>();
		probetable = new HashMap<Object, ArrayList<Tuple>>();

		// Initialize cursors
		lcurs = 0;
		rcurs = 0;
		kcurs = 0; // this cursors keep track of the current searchKey;
		
		eosl = false;
		eosr = false;
		complete = false;
		built = false;
		
		if (!right.open()) {
			return false;
		} 
		else {
			filenum++;
			rfname = "Temp" + String.valueOf(filenum) + "_HashJoin";
			
			// Materialize the RHS into a file
			try {
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
				while ((rightpage = right.next()) != null) {
					out.writeObject(rightpage);
				}
				out.close();
			} 
			catch (IOException io) {
				System.out.println("HashJoin: writing the temporary file error");
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
//		 System.out.print("HashJoin:--------------------------in next----------------");
//		 Debug.PPrint(con);
//		 System.out.println();
		
		// Return if finished
		if (complete || eosl) {
			close();
			return null;
		}
		
		// Phase 1: Partitioning
		// Partition the left table
		Batch leftbatch = left.next();
		while (leftbatch != null) {
			for (int i = 0; i < leftbatch.size(); ++i) {
				Tuple leftTuple = leftbatch.elementAt(i);

				int key = hash1(leftTuple.dataAt(leftindex));
				if (lefttable.containsKey(key)) { 
					lefttable.get(key).add(leftTuple);
				} 
				else { // doesn't exist already, create a new list
					ArrayList<Tuple> leftTupleList = new ArrayList<Tuple>();
					leftTupleList.add(leftTuple);
					lefttable.put(key, leftTupleList);
				}
			}
			leftbatch = left.next();
		}
		
		// Mark left table as partitioned
		eosl = true;
		
		// Partition the right table
		if (eosr == false) {
			try {
				// Open the input stream
				in = new ObjectInputStream(new FileInputStream(rfname));
			} 
			catch (IOException io) {
				io.printStackTrace();
				System.exit(1);
			}
			
			try {
				Batch rightbatch = (Batch) in.readObject();
				while (rightbatch != null) {
					for (int i = 0; i < rightbatch.size(); ++i) {
						Tuple rightTuple = rightbatch.elementAt(i);
						
						int key = hash1(rightTuple.dataAt(rightindex));
						if (righttable.containsKey(key)) {
							righttable.get(key).add(rightTuple);
						} 
						else {
							ArrayList<Tuple> rightTupleList = new ArrayList<Tuple>();
							rightTupleList.add(rightTuple);
							righttable.put(key, rightTupleList);
						}
					}
					rightbatch = (Batch) in.readObject();
				}
			} 
			catch (EOFException e) {
				try {
					in.close();
				} 
				catch (IOException io) {
					System.out.println("HashJoin:Error in temporary file reading");
				}
				eosr = true;
			} 
			catch (ClassNotFoundException c) {
				System.out.println("HashJoin:Some error in deserialization ");
				System.exit(1);
			} 
			catch (IOException io) {
				System.out.println("HashJoin:temporary file reading error");
				System.exit(1);
			}
			
			// Mark right table as partitioned
			eosr = true;
		}
		
		// Phase 2: Probing		

		// Temp cursors to iterate over
		int l, r, k;
		
		// Initialize keyset
		leftkeys = new ArrayList<Object>(lefttable.keySet());
		
		// Page to hold matching tuples
		outbatch = new Batch(batchsize);
		
		// While there are still keys for probing
		while (!outbatch.isFull() && kcurs < leftkeys.size()) {
			for (k = kcurs; k < leftkeys.size(); ++k) {
				Object key = leftkeys.get(k);
				
				if (righttable.containsKey(key)) { 
					ArrayList<Tuple> leftTupleList = lefttable.get(key);
					ArrayList<Tuple> rightTupleList = righttable.get(key);

					// built a hash table on hash2() using key
					if (!built) {
						for (int i = 0; i < leftTupleList.size(); ++i) {
							Tuple leftTuple = leftTupleList.get(i);
							int hash = hash2(leftTuple.dataAt(leftindex));
							
							// Add to probetable for join with right table
							if (probetable.containsKey(hash)) {
								probetable.get(hash).add(leftTuple);
							} 
							else {
								ArrayList<Tuple> list = new ArrayList<Tuple>();
								list.add(leftTuple);
								probetable.put(hash, list);
							}
						}
						built = true;
					}
				
					// Hash the right tuple and join all the matching tuples 
					for (r = rcurs; r < rightTupleList.size(); ++r) {           
						Tuple rightTuple = rightTupleList.get(r);
						
						int hash = hash2(rightTuple.dataAt(rightindex));
						if (probetable.containsKey(hash)) {
							ArrayList<Tuple> list = probetable.get(hash);
							
							for (l = lcurs; l < list.size(); ++l) {
								Tuple leftTuple = list.get(l);
								Tuple outputTuple = leftTuple.joinWith(rightTuple);
								Debug.PPrint(outputTuple);
								outbatch.add(outputTuple);
					
								if (outbatch.isFull()) { 
									if (l == list.size() - 1 && r == rightTupleList.size() - 1) { // end of probe list and right partition
										lcurs = 0;
										rcurs = 0;
										kcurs = k + 1;
										built = false;
										probetable.clear();
									} 
									else if (r != rightTupleList.size() - 1 && l == list.size() - 1) { // haven't reached end of right partition
										rcurs = r + 1;
										lcurs = 0;
									} 
									else { // haven't reached end of probe list
										lcurs = l + 1;
									}
									return outbatch;
								}
							}
						}
						rcurs++;
						lcurs = 0;  
					}
				}
				kcurs++;
				lcurs = 0;
				rcurs = 0;
				built = false;
				probetable.clear(); 
			}
		}
		kcurs++;
		lcurs = 0;
		rcurs = 0;
		built = false;
		probetable.clear(); 

		// If no more keys, terminate
		if (kcurs >= leftkeys.size()) {
			complete = true;
		}
		
		return outbatch;
	}
	
	// Close the operator
	public boolean close() {
		File f = new File(rfname);
		f.delete();
		return true;
	}
	
	// Hash function for partitioning phase
	private static int hash1(Object o) {
        return toInt(o) * 47459 % 65537;
    }
	
	// Hash function for probing phase
	private static int hash2(Object o) {
        return toInt(o) * 47563 % 65539;
    }
	
	// Utility function to cast objects to integers
    protected static int toInt(Object o) {
        return Integer.valueOf(String.valueOf(o));
    }
}