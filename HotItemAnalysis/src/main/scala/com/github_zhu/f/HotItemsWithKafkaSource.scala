package com.github_zhu.f

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable


/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2021/4/12 20:37
 * @ModifiedBy:
 *
 */

//输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, catageoryId: Int, behavior: String, timestamp: Long)

//中间结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItemsWithKafkaSource {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop112:9092")
    properties.setProperty("group.id", "consumer-group")

    //从kafka 读取数据
    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

    //转换为样例类
    val dataStream :DataStream[UserBehavior] = inputStream.
      map(data=>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })

      //默认 assignAscendingTimestamps 会延迟一毫秒
      //return new Watermark(if (currentTimestamp == Long.MIN_VALUE)  { Long.MIN_VALUE}else{currentTimestamp - 1})
      .assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L)

    //进行窗口操作
    val aggStream: DataStream[ItemViewCount] = dataStream.filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult())


    // 对统计结果按窗口分组，排序输出
    val resultStream: DataStream[String] =
      aggStream.keyBy("windowEnd")
        .process(new TopNItems(5))

    resultStream.print()

    env.execute("hor")

  }

}

//zidingyi  yujuhehanshu
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1
  override def getResult(accumulator: Long): Long = accumulator
  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windEnd = window.getEnd
    val count = input.iterator.next()

    out.collect(ItemViewCount(itemId, windEnd, count))

  }
}

// 自定义一个keyedProcessFunction 对每个窗口的count 统计值排序 并格式化成字符串输出
class TopNItems(topActor: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //自定义一个列表状态 用来保存当前窗口中所有商品的count 值
  private var itemValueistState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemValueistState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemValueistState", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //没来一条数据 添加到listStatezhong
    itemValueistState.add(value)

    //注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    /*//遍历list中数据 放到listBuffer中
     val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
     import scala.collection.JavaConversions._
     for (item <- itemValueistState.get()) {
       allItemViewCounts.add(item)
     }
 */
    var allValueList: scala.collection.immutable.List[ItemViewCount] = scala.collection.immutable.List[ItemViewCount]()
    import scala.collection.JavaConversions._
    allValueList = itemValueistState.get().toList

    itemValueistState.clear()

    val sortedItemViewCounts = allValueList.sortBy(_.count)(Ordering.Long.reverse).take(topActor)

    //将排序后的数据包装成可视化String 便于打印输出
    val result: mutable.StringBuilder = new mutable.StringBuilder()

    result.append("========\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    //遍历结果数据，将每个ItemViewCount 的商品的ID值和count值及排名输出
    for (i <- sortedItemViewCounts.indices) {
      val currentViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i).append(",").append("商品id:").append(currentViewCount.itemId).append(",")
        .append("商品count：").append(currentViewCount.count)
        .append("\n")
    }
    Thread.sleep(1000)

    out.collect(result.toString())

  }
}