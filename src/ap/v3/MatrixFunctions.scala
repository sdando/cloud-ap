package ap.v3

import org.jblas.DoubleMatrix

object MatrixFunctions {
  
  def aggregate(matrix: DoubleMatrix) = {
    var accumulator = DoubleMatrix.zeros(matrix.getRows())
    for(i <- 0 until matrix.getColumns())
    {
      accumulator.addi(matrix.getColumn(i))
    }
    accumulator
  }

  def main(args: Array[String]) {
    val matrix = new DoubleMatrix(Array(1.2, 10.1, 3.1, 4.1))
    val sumMatrix = aggregate(matrix)
    println(sumMatrix.get(0,0))
  }
  
}