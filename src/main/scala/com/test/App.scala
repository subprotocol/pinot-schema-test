package com.test

import java.io.File

import com.linkedin.pinot.common.utils.{KafkaStarterUtils, TenantRole}
import com.linkedin.pinot.tools.admin.command._
import kafka.server.KafkaServerStartable
import org.apache.commons.io.FileUtils

object App {

  val clusterName = "test-cluster"
  val numServers = 3
  private val tempDir = new File("/tmp", String.valueOf(System.currentTimeMillis))
  private var kafka: KafkaServerStartable = _


  def main(args: Array[String]): Unit = {
    startup()

    addSchema(pathToResource("events.json"))


    // case 1: schema: events, table: events
    addTable(pathToResource("table_config_case1.json"))


    // sleep for 5 seconds so the first table comes online and doesn't mix logging
    // messages with the next tables creation
    Thread.sleep(5000)

    // case 3: schema: events, table: events2
    addTable(pathToResource("table_config_case3.json"))

    println("sleeping..")
    Thread.sleep(100000)
  }

  def pathToResource(path: String): String = {
    getClass.getResource(s"/$path").getPath
  }

  def addSchema(path: String): Boolean = {
    new AddSchemaCommand()
      .setControllerHost("localhost")
      .setControllerPort("9000")
      .setSchemaFilePath(path)
      .setExecute(true)
      .execute()
  }

  def addTable(path: String): Boolean= {
    new AddTableCommand()
      .setFilePath(path)
      .setControllerPort("9000")
      .setExecute(true)
      .execute()
  }

  def startup(): Unit = {
    startZookeeper()
    startControllers()
    startBrokers("broker", 1)
    startServers("server", numServers)
    startKafka()
  }

  def shutdown(): Unit = {

    new StopProcessCommand(false)
      .stopController()
      .stopBroker()
      .stopServer()
      .stopZookeeper()
      .execute()

    kafka.shutdown()

    FileUtils.cleanDirectory(tempDir)
  }

  private def startZookeeper(): Unit = {
    new StartZookeeperCommand()
      .execute()
  }

  private def startControllers(): Unit = {
    new StartControllerCommand()
      .setControllerPort("9000")
      .setZkAddress("localhost:2181")
      .setClusterName(clusterName)
      .setTenantIsolation(false)
      .execute()
  }

  private def startBrokers(tenantName: String, number: Int): Unit = {

    for (i <- 0 until number) {
      new StartBrokerCommand()
        .setZkAddress("localhost:2181")
        .setClusterName(clusterName)
        .setPort(8000 + i)
        .execute()

      new AddTenantCommand()
        .setControllerUrl("http://localhost:9000")
        .setName(tenantName)
        .setInstances(number)
        .setRole(TenantRole.BROKER)
        .setExecute(true)
        .execute()
    }

  }

  private def startServers(tenantName: String, numRealtime: Int): Unit = {
    for (i <- 0 until numRealtime) {
      new StartServerCommand()
        .setAdminPort(7500+i)
        .setPort(7000+i)
        .setZkAddress("localhost:2181")
        .setClusterName(clusterName)
        .setDataDir(new File(tempDir, "PinotServerData" + i).getAbsolutePath)
        .setSegmentDir(new File(tempDir, "PinotServerSegment" + i).getAbsolutePath)
        .execute()
    }

    new AddTenantCommand().setControllerUrl("http://localhost:9000")
      .setName(tenantName)
      .setOffline(0)
      .setRealtime(numRealtime)
      .setInstances(numRealtime)
      .setRole(TenantRole.SERVER)
      .setExecute(true)
      .execute()
  }

  private def startKafka(): Unit = {
    kafka = KafkaStarterUtils.startServer(9092, 0, "localhost:2181", KafkaStarterUtils.getDefaultKafkaConfiguration)
  }
}
