package com.study.spark.CK.api

import java.util.Properties

import com.study.spark.CK.constants.Constants
import com.study.spark.CK.utils.InitSpark
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
 * @author feng xl
 * @date 2021/6/20 0020 20:30
 */

/**
 * 简单读写ck的API
 * */

class CkApi {

    val spark: SparkSession = InitSpark.sparkSession

    var tableName: String = Constants.TABLE_NAME

    /**
     * 查询默认表
     * */

    def select(): Unit = {
        spark.read
          .format("jdbc")
          .option("driver", Constants.DRIVER_NAME)
          .option("url", Constants.JDBC_URL)
          .option("numPartitions", Constants.NUM_OF_PARTITIONS)
          .option("dbtable", tableName)
          .load().show()
    }
    /**
     * 查询指定表
     * */
    def select(tableName: String): Unit = {
        this.tableName = tableName
        select()
        this.tableName = Constants.TABLE_NAME
    }
    /**
     * 插入默认表
     * */
    def insert(dataFrame: DataFrame): Unit = {
        val properties = new Properties()
        properties.put("user", "default")
        dataFrame.write
          .mode(SaveMode.Append)
          .option("batchsize", Constants.BATCH_SIZE)
          .option("isolationLevel", "NONE")
          .option("numPartitions", Constants.NUM_OF_PARTITIONS)
          .jdbc(Constants.JDBC_URL, this.tableName, properties)
    }
    /**
     * 插入指定表
     * */
    def insert(dataFrame: DataFrame, tableName: String): Unit = {
        this.tableName = tableName
        insert(dataFrame)
        this.tableName = Constants.TABLE_NAME
    }
}
