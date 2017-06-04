import org.apache.spark.SparkContext

/**
  * Created by Edward on 2016-12-22.
  */
object Aggregate {
  val sc = new SparkContext()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  var rdd1 = sc.makeRDD(1 to 10,2)
  rdd1.mapPartitionsWithIndex{
    (partIdx, iter)=>{
      val part_map = scala.collection.mutable.Map[String, List[Int]]()
      while(iter.hasNext) {
        val part_name = "part_" + partIdx
        var elem = iter.next()
        if(part_map.contains(part_name)) {
          var elems = part_map(part_name)
          elems ::= elem
          part_map(part_name) = elems
        } else {
          part_map(part_name) = List[Int]{elem}
        }
      }
      part_map.iterator
    }
  }.collect

  rdd1.aggregate(1)(
    {(x:Int, y:Int)=>x+y},{(a:Int, b:Int)=>a+b}
  )
}
