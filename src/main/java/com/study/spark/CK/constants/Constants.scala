package com.study.spark.CK.constants

/**
 * @author feng xl
 * @date 2021/6/20 0020 20:22
 */

/**
 * 连接所需的常量
 *
 * */
object Constants {

    val DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver"
    val DATABASE_NAME = "test"
    val JDBC_URL = s"jdbc:clickhouse://hadoop01:8123/$DATABASE_NAME"
    val USER = "default"
    val TABLE_NAME = "test_table1"
    val BATCH_SIZE = "100000"
    val SOCKET_TIMEOUT = "300000"
    val NUM_OF_PARTITIONS = "3"
    val REWRITE_BATCHED_STATEMENTS = "true"
}
