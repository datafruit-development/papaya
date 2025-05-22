package datafruit

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin._

class TestPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new TestDriver
  override def executorPlugin(): ExecutorPlugin = new TestExecutor
}

class TestDriver extends DriverPlugin {
  override def init(sc: SparkContext, ctx: PluginContext): java.util.Map[String, String] = {
    println("[PLUGIN] Driver initialized")
    new java.util.HashMap[String, String]()
  }
}

class TestExecutor extends ExecutorPlugin {
  override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    println("[PLUGIN] Executor initialized")
  }
}
