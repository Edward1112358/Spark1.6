import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by Edward on 2017/6/6.
  */
class MyUDAF extends UserDefinedAggregateFunction {
  // 该方法指定具体输入数据的类型
  override def inputSchema: StructType = StructType(Array(StructField("input", StringType, true)))
  //在进行聚合操作的时候所要处理的数据的结果的类型
  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, true)))
  //指定UDAF函数计算后返回的结果类型
  override def dataType: DataType = IntegerType
  // 确保一致性 一般用true
  override def deterministic: Boolean = true
  //在Aggregate之前每组数据的初始化结果
  override def initialize(buffer: MutableAggregationBuffer): Unit = {buffer(0) =0}
  // 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
  // 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }
  //最后在分布式节点进行Local Reduce完成后需要进行全局级别的Merge操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }
  //返回UDAF最后的计算结果
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}

