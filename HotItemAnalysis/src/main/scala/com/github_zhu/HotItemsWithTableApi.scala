package com.github_zhu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2021/4/14 15:47
 * @ModifiedBy:
 *
 */
object HotItemsWithTableApi {
  def main(args: Array[String]): Unit = {

    //创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //创建表执行环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tablEnv = StreamTableEnvironment.create(env, settings)

    //从文件中读取数据
    val inputStream: DataStream[String] = env.readTextFile("E:\\javaworkingspace\\scala\\UserBehavior\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //转换成样例类并分配时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      })
      .assignAscendingTimestamps(_.timestamp*1000L)

    //把流转换成表
    val dataTable = tablEnv.fromDataStream(dataStream,'itemId,'behavior,   'timestamp.rowtime as 'ts)

    //进行开窗聚合操作 ， 调用 tableAPI
    val aggTable = dataTable
      .filter('behavior==="pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw )
      .groupBy('itemId,'sw)
      .select('itemId,'sw.end as 'windowEnd,'itemId.count as 'cnt)

    //将聚合结果表注册到环境 ，写sql 实现TopN
    tablEnv.createTemporaryView("agg",aggTable,'itemId,'cnt,'windowEnd)
    val resultTable = tablEnv.sqlQuery(
      """
        |select *
        |from(
        |     select * , row_number() over (partition by windowEnd order by cnt desc) as row_num
        |     from agg
        |   )
        |where row_num<=5
        |""".stripMargin)

    aggTable.toAppendStream[Row].print("agg")
    resultTable.toRetractStream[Row].print("result")

    env.execute("hot items with table api TopN")

  }
}
