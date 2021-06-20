package com.study.spark.CK.utils

import java.util.Properties

import com.study.spark.CK.constants.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource}

/**
 * @author feng xl
 * @date 2021/6/18 0018 0:01
 */

/**
 * 全局上下文
 * */
object InitSpark {

    val sparkConf: SparkConf =  new SparkConf().setAppName("spark-ck").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    def stop(): Unit = {
        sparkSession.stop()
    }

}
