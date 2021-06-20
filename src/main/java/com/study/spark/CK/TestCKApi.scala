package com.study.spark.CK

import com.study.spark.CK.api.CkApi
import com.study.spark.CK.model.Info
import com.study.spark.CK.utils.InitSpark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author feng xl
 * @date 2021/6/20 0020 20:14
 */

object TestCKApi {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = InitSpark.sparkSession
        val ckApi = new CkApi()
        /**     测试查询    */
//        ckApi.select()

        /**    测试写入指定表*/
//        val frame: DataFrame = spark.read
//          .option("header", "true")
//          .format("csv")
//          .load("info.csv")
//        ckApi.insert(frame, "spark_table1")
//        ckApi.select("spark_table1")
        InitSpark.stop()
    }
}
