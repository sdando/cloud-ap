package ap.v2;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

public class MatrixFunctions {
	public static Vector aggregate(Matrix matrix){
		Vector accumulator=new RandomAccessSparseVector(matrix.numCols());
		for(int i=0;i<matrix.numRows();i++){
			accumulator.assign(matrix.viewRow(i), Functions.PLUS);
		}  
		return accumulator;
	}

}
