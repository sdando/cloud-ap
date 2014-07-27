
class MySample(var a:Int,var b:Int){
  println("create Sample")
  def printA(s:String) = s+a 
}

object TestSc {

  def main(args: Array[String]): Unit = {
    println("hello,my scala!")
    val o=new MySample(1,2)
    println(o.printA("a is "))
  }

}