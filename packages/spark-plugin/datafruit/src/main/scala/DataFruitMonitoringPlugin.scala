package datafruit

import java.lang.management.ManagementFactory
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Instant
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sun.management.OperatingSystemMXBean
import org.apache.spark.api.plugin._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.{SparkContext, SparkEnv}
import scala.collection.mutable
import scala.concurrent.duration._

class DataFruitMonitoringPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new DataFruitDriverPlugin
  override def executorPlugin(): ExecutorPlugin = new DataFruitExecutorPlugin
}

sealed trait DataFruitMessage extends Serializable
case class CpuSnapshotMessage(snap: CpuSnapshot) extends DataFruitMessage
case class MemorySnapshotMessage(snap: MemorySnapshot) extends DataFruitMessage

case class PerformanceIssue(
  issueType: String,
  severity: String,
  description: String,
  recommendation: String,
  detectedAt: Long,
  metrics: Map[String, Any]
)

object ExceptionClassifier {
  sealed trait ExceptionCategory
  case object InfrastructureFailure extends ExceptionCategory
  case object DataFailure extends ExceptionCategory
  case object UserCodeFailure extends ExceptionCategory
  case object ResourceFailure extends ExceptionCategory
  case object NetworkFailure extends ExceptionCategory
  case object Unknown extends ExceptionCategory

  case class ClassifiedException(
    category: ExceptionCategory,
    className: String,
    description: String,
    stackTrace: String,
    fullStackTrace: Array[StackTraceElement],
    timestamp: Long,
    taskId: Option[Long] = None,
    stageId: Option[Int] = None,
    jobId: Option[Int] = None
  )

  def classify(exception: String, stackTrace: String): ExceptionCategory = {
    val lowerException = exception.toLowerCase
    val lowerStackTrace = stackTrace.toLowerCase

    if (lowerException.contains("outofmemory") ||
        lowerException.contains("java.lang.outofmemoryerror") ||
        lowerStackTrace.contains("gc overhead limit exceeded")) {
      ResourceFailure
    } else if (lowerException.contains("connection") ||
               lowerException.contains("timeout") ||
               lowerException.contains("network") ||
               lowerException.contains("socket")) {
      NetworkFailure
    } else if (lowerException.contains("filenotfound") ||
               lowerException.contains("accessdenied") ||
               lowerException.contains("permissiondenied") ||
               lowerException.contains("ioexception")) {
      InfrastructureFailure
    } else if (lowerException.contains("numberformat") ||
               lowerException.contains("parseexception") ||
               lowerException.contains("illegalargument") ||
               lowerException.contains("nullpointer")) {
      DataFailure
    } else if (lowerStackTrace.contains("user") ||
               lowerStackTrace.contains("main") ||
               lowerStackTrace.contains("lambda")) {
      UserCodeFailure
    } else {
      Unknown
    }
  }
}

case class CpuSnapshot(
  timestamp: Long,
  executorId: String,
  processCpuTime: Long,
  systemCpuLoad: Double,
  processCpuLoad: Double,
  availableProcessors: Int
) extends Serializable

case class MemorySnapshot(
  timestamp: Long,
  executorId: String,
  heapUsed: Long,
  heapCommitted: Long,
  heapMax: Long,
  offHeapUsed: Long,
  offHeapCommitted: Long,
  storageMemoryUsed: Long,
  storageMemoryMax: Long,
  executionMemoryUsed: Long,
  executionMemoryMax: Long,
  totalSystemMemory: Long,
  freeSystemMemory: Long
) extends Serializable

case class GcSnapshot(
  timestamp: Long,
  executorId: String,
  gcName: String,
  collectionCount: Long,
  collectionTime: Long
) extends Serializable

class SparkMetricsCollector {
  val jobsSubmitted = new AtomicLong(0)
  val jobsSucceeded = new AtomicLong(0)
  val jobsFailed = new AtomicLong(0)
  val stagesSubmitted = new AtomicLong(0)
  val stagesCompleted = new AtomicLong(0)
  val stagesFailed = new AtomicLong(0)
  val stagesSkipped = new AtomicLong(0)
  val tasksLaunched = new AtomicLong(0)
  val tasksCompleted = new AtomicLong(0)
  val tasksFailed = new AtomicLong(0)
  val tasksKilled = new AtomicLong(0)
  val tasksSkipped = new AtomicLong(0)

  val executorsAdded = new AtomicLong(0)
  val executorsRemoved = new AtomicLong(0)
  val executorsBlacklisted = new AtomicLong(0)
  val executorsUnblacklisted = new AtomicLong(0)

  val gcTimeTotal = new AtomicLong(0)
  val ioTimeTotal = new AtomicLong(0)
  val shuffleReadBytes = new AtomicLong(0)
  val shuffleWriteBytes = new AtomicLong(0)
  val diskSpillBytes = new AtomicLong(0)
  val memorySpillBytes = new AtomicLong(0)

  private val cpuSnapshots = new mutable.ListBuffer[CpuSnapshot]()
  private val memorySnapshots = new mutable.ListBuffer[MemorySnapshot]()

  private val exceptions = new mutable.ListBuffer[ExceptionClassifier.ClassifiedException]()
  private val taskRetryPattern = new ConcurrentHashMap[String, AtomicInteger]()

  private val performanceIssues = new mutable.ListBuffer[PerformanceIssue]()

  def getJobSuccessRate: Double = {
    val total = jobsSubmitted.get()
    if (total == 0) 1.0 else jobsSucceeded.get().toDouble / total
  }

  def getStageSuccessRate: Double = {
    val total = stagesSubmitted.get()
    if (total == 0) 1.0 else stagesCompleted.get().toDouble / total
  }

  def getTaskSuccessRate: Double = {
    val total = tasksLaunched.get()
    if (total == 0) 1.0 else tasksCompleted.get().toDouble / total
  }

  def incrementJobSubmitted(): Unit = jobsSubmitted.incrementAndGet()
  def incrementJobSucceeded(): Unit = jobsSucceeded.incrementAndGet()
  def incrementJobFailed(): Unit = jobsFailed.incrementAndGet()

  def incrementStageSubmitted(): Unit = stagesSubmitted.incrementAndGet()
  def incrementStageCompleted(): Unit = stagesCompleted.incrementAndGet()
  def incrementStageFailed(): Unit = stagesFailed.incrementAndGet()
  def incrementStageSkipped(): Unit = stagesSkipped.incrementAndGet()

  def incrementTaskLaunched(): Unit = tasksLaunched.incrementAndGet()
  def incrementTaskCompleted(): Unit = tasksCompleted.incrementAndGet()
  def incrementTaskFailed(): Unit = tasksFailed.incrementAndGet()
  def incrementTaskKilled(): Unit = tasksKilled.incrementAndGet()
  def incrementTaskSkipped(): Unit = tasksSkipped.incrementAndGet()

  def addException(exception: ExceptionClassifier.ClassifiedException): Unit = {
    exceptions.synchronized {
      exceptions += exception
    }
  }

  def addCpuSnapshot(snap: CpuSnapshot): Unit = cpuSnapshots.synchronized {
    cpuSnapshots += snap
  }

  def addMemorySnapshot(snap: MemorySnapshot): Unit = memorySnapshots.synchronized {
    memorySnapshots += snap
  }

  def trackTaskRetry(taskKey: String): Int = {
    taskRetryPattern.computeIfAbsent(taskKey, _ => new AtomicInteger(0)).incrementAndGet()
  }

  def addPerformanceIssue(issue: PerformanceIssue): Unit = {
    performanceIssues.synchronized {
      performanceIssues += issue
    }
  }

  def getMetricsSummary: Map[String, Any] = {
    Map(
      "jobs" -> Map(
        "submitted" -> jobsSubmitted.get(),
        "succeeded" -> jobsSucceeded.get(),
        "failed" -> jobsFailed.get(),
        "successRate" -> getJobSuccessRate
      ),
      "stages" -> Map(
        "submitted" -> stagesSubmitted.get(),
        "completed" -> stagesCompleted.get(),
        "failed" -> stagesFailed.get(),
        "skipped" -> stagesSkipped.get(),
        "successRate" -> getStageSuccessRate
      ),
      "tasks" -> Map(
        "launched" -> tasksLaunched.get(),
        "completed" -> tasksCompleted.get(),
        "failed" -> tasksFailed.get(),
        "killed" -> tasksKilled.get(),
        "skipped" -> tasksSkipped.get(),
        "successRate" -> getTaskSuccessRate
      ),
      "executors" -> Map(
        "added" -> executorsAdded.get(),
        "removed" -> executorsRemoved.get(),
        "blacklisted" -> executorsBlacklisted.get(),
        "unblacklisted" -> executorsUnblacklisted.get()
      ),
      "performance" -> Map(
        "gcTimeTotalMs" -> gcTimeTotal.get(),
        "ioTimeTotalMs" -> ioTimeTotal.get(),
        "shuffleReadBytes" -> shuffleReadBytes.get(),
        "shuffleWriteBytes" -> shuffleWriteBytes.get(),
        "diskSpillBytes" -> diskSpillBytes.get(),
        "memorySpillBytes" -> memorySpillBytes.get()
      ),
      "snapshots" -> Map(
        "cpuSnapshotsCollected" -> cpuSnapshots.size,
        "memorySnapshotsCollected" -> memorySnapshots.size
      ),
      "exceptions" -> exceptions.length,
      "performanceIssues" -> performanceIssues.length
    )
  }

  def getWebhookPayload(apiKey: String): Map[String, Any] = {
    Map(
      "api_key" -> apiKey,
      "timestamp" -> System.currentTimeMillis(),
      "application_id" -> SparkContext.getOrCreate().applicationId,
      "application_name" -> SparkContext.getOrCreate().appName,
      "metrics" -> getMetricsSummary,
      "recent_exceptions" -> getRecentExceptions(10),
      "active_performance_issues" -> getActivePerformanceIssues,
      "recent_cpu_snapshots" -> getRecentCpuSnapshots(5),
      "recent_memory_snapshots" -> getRecentMemorySnapshots(5),
      "system_health" -> getSystemHealthSummary
    )
  }

  def getRecentExceptions(limit: Int): List[Map[String, Any]] = {
    exceptions.synchronized {
      exceptions.takeRight(limit).map { ex =>
        Map(
          "category" -> ex.category.toString,
          "className" -> ex.className,
          "description" -> ex.description,
          "timestamp" -> ex.timestamp,
          "taskId" -> ex.taskId.getOrElse("N/A"),
          "stageId" -> ex.stageId.getOrElse("N/A"),
          "jobId" -> ex.jobId.getOrElse("N/A")
        )
      }.toList
    }
  }

  def getActivePerformanceIssues: List[Map[String, Any]] = {
    performanceIssues.synchronized {
      performanceIssues.filter { issue =>
        System.currentTimeMillis() - issue.detectedAt < 300000
      }.map { issue =>
        Map(
          "issueType" -> issue.issueType,
          "severity" -> issue.severity,
          "description" -> issue.description,
          "recommendation" -> issue.recommendation,
          "detectedAt" -> issue.detectedAt,
          "metrics" -> issue.metrics
        )
      }.toList
    }
  }

  def getRecentCpuSnapshots(limit: Int): List[Map[String, Any]] = {
    cpuSnapshots.synchronized {
      cpuSnapshots.takeRight(limit).map { snap =>
        Map(
          "timestamp" -> snap.timestamp,
          "executorId" -> snap.executorId,
          "systemCpuLoad" -> snap.systemCpuLoad,
          "processCpuLoad" -> snap.processCpuLoad,
          "availableProcessors" -> snap.availableProcessors
        )
      }.toList
    }
  }

  def getRecentMemorySnapshots(limit: Int): List[Map[String, Any]] = {
    memorySnapshots.synchronized {
      memorySnapshots.takeRight(limit).map { snap =>
        Map(
          "timestamp" -> snap.timestamp,
          "executorId" -> snap.executorId,
          "heapUsedMB" -> snap.heapUsed / (1024 * 1024),
          "heapMaxMB" -> snap.heapMax / (1024 * 1024),
          "heapUtilization" -> (snap.heapUsed.toDouble / snap.heapMax * 100),
          "storageMemoryUsedMB" -> snap.storageMemoryUsed / (1024 * 1024),
          "executionMemoryUsedMB" -> snap.executionMemoryUsed / (1024 * 1024)
        )
      }.toList
    }
  }

  def getSystemHealthSummary: Map[String, Any] = {
    val currentTime = System.currentTimeMillis()
    val last5Minutes = currentTime - 300000

    val recentFailures = exceptions.count(_.timestamp > last5Minutes)
    val recentIssues = performanceIssues.count(_.detectedAt > last5Minutes)
    
    val recentCpuSnapshots = cpuSnapshots.filter(_.timestamp > last5Minutes)
    val recentMemSnapshots = memorySnapshots.filter(_.timestamp > last5Minutes)
    
    val avgCpuLoad = if (recentCpuSnapshots.nonEmpty) {
      recentCpuSnapshots.map(_.systemCpuLoad).sum / recentCpuSnapshots.length
    } else 0.0

    val avgMemoryUtil = if (recentMemSnapshots.nonEmpty) {
      recentMemSnapshots.map(snap => snap.heapUsed.toDouble / snap.heapMax * 100).sum / recentMemSnapshots.length
    } else 0.0

    Map(
      "status" -> determineHealthStatus(recentFailures, recentIssues, avgCpuLoad, avgMemoryUtil),
      "recentFailures" -> recentFailures,
      "recentPerformanceIssues" -> recentIssues,
      "averageCpuLoad" -> avgCpuLoad,
      "averageMemoryUtilization" -> avgMemoryUtil,
      "activeExecutors" -> getActiveExecutorCount
    )
  }

  private def determineHealthStatus(failures: Int, issues: Int, cpuLoad: Double, memUtil: Double): String = {
    if (failures > 5 || issues > 3 || cpuLoad > 90 || memUtil > 85) "CRITICAL"
    else if (failures > 2 || issues > 1 || cpuLoad > 70 || memUtil > 70) "WARNING"
    else "HEALTHY"
  }

  private def getActiveExecutorCount: Int = {
    (executorsAdded.get() - executorsRemoved.get()).toInt
  }
}

class DataFruitSparkListener(metricsCollector: SparkMetricsCollector) extends SparkListener {

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    metricsCollector.incrementJobSubmitted()
    println(s"[DATAFRUIT] Job ${jobStart.jobId} started with ${jobStart.stageIds.length} stages")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val resultString = jobEnd.jobResult.toString
    
    if (resultString.contains("JobSucceeded")) {
      metricsCollector.incrementJobSucceeded()
      println(s"[DATAFRUIT] Job ${jobEnd.jobId} succeeded")
    } else {
      metricsCollector.incrementJobFailed()
      
      val exception = new RuntimeException(s"Job failed: $resultString")
      
      val classifiedException = ExceptionClassifier.ClassifiedException(
        category = ExceptionClassifier.classify(resultString, ""),
        className = "JobFailure",
        description = resultString,
        stackTrace = resultString,
        fullStackTrace = Array.empty,
        timestamp = System.currentTimeMillis(),
        jobId = Some(jobEnd.jobId)
      )

      metricsCollector.addException(classifiedException)
      println(s"[DATAFRUIT] Job ${jobEnd.jobId} failed: $resultString")
      detectJobFailurePatterns(jobEnd.jobId, exception)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    metricsCollector.incrementStageSubmitted()
    println(s"[DATAFRUIT] Stage ${stageSubmitted.stageInfo.stageId} submitted")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo

    stageInfo.failureReason match {
      case Some(reason) =>
        metricsCollector.incrementStageFailed()

        val classifiedException = ExceptionClassifier.ClassifiedException(
          category = ExceptionClassifier.classify(reason, reason),
          className = "StageFailure",
          description = reason,
          stackTrace = reason,
          fullStackTrace = Array.empty,
          timestamp = System.currentTimeMillis(),
          stageId = Some(stageInfo.stageId)
        )

        metricsCollector.addException(classifiedException)
        println(s"[DATAFRUIT] Stage ${stageInfo.stageId} failed: $reason")

      case None =>
        if (stageInfo.numTasks == 0) {
          metricsCollector.incrementStageSkipped()
          println(s"[DATAFRUIT] Stage ${stageInfo.stageId} skipped")
        } else {
          metricsCollector.incrementStageCompleted()
          println(s"[DATAFRUIT] Stage ${stageInfo.stageId} completed successfully")
        }
    }
    analyzeStagePerformance(stageInfo)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    metricsCollector.incrementTaskLaunched()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val reasonString = taskEnd.reason.toString
    val reasonClass = taskEnd.reason.getClass.getSimpleName
    
    if (reasonClass == "Success") {
      metricsCollector.incrementTaskCompleted()
      analyzeTaskPerformance(taskEnd)
    } else if (reasonClass == "TaskKilled") {
      metricsCollector.incrementTaskKilled()
      println(s"[DATAFRUIT] Task ${taskEnd.taskInfo.taskId} killed. Reason: $reasonString")
    } else {
      metricsCollector.incrementTaskFailed()

      val taskKey = s"${taskEnd.stageId}-${taskEnd.taskInfo.index}"
      val retryCount = metricsCollector.trackTaskRetry(taskKey)

      val classifiedException = if (reasonClass == "ExceptionFailure") {
        ExceptionClassifier.ClassifiedException(
          category = ExceptionClassifier.classify(reasonString, reasonString),
          className = "ExceptionFailure",
          description = reasonString,
          stackTrace = reasonString,
          fullStackTrace = Array.empty,
          timestamp = System.currentTimeMillis(),
          taskId = Some(taskEnd.taskInfo.taskId),
          stageId = Some(taskEnd.stageId)
        )
      } else if (reasonClass == "ExecutorLostFailure") {
        ExceptionClassifier.ClassifiedException(
          category = ExceptionClassifier.InfrastructureFailure,
          className = "ExecutorLostFailure",
          description = s"Executor lost: $reasonString",
          stackTrace = reasonString,
          fullStackTrace = Array.empty,
          timestamp = System.currentTimeMillis(),
          taskId = Some(taskEnd.taskInfo.taskId),
          stageId = Some(taskEnd.stageId)
        )
      } else {
        ExceptionClassifier.ClassifiedException(
          category = ExceptionClassifier.Unknown,
          className = reasonClass,
          description = reasonString,
          stackTrace = reasonString,
          fullStackTrace = Array.empty,
          timestamp = System.currentTimeMillis(),
          taskId = Some(taskEnd.taskInfo.taskId),
          stageId = Some(taskEnd.stageId)
        )
      }

      metricsCollector.addException(classifiedException)

      if (retryCount >= 4) {
        val issue = PerformanceIssue(
          issueType = "TaskRetryPattern",
          severity = "CRITICAL",
          description = s"Task $taskKey has failed $retryCount times, job will fail",
          recommendation = "Investigate task failure root cause and consider data partitioning",
          detectedAt = System.currentTimeMillis(),
          metrics = Map("retryCount" -> retryCount, "taskKey" -> taskKey)
        )
        metricsCollector.addPerformanceIssue(issue)
      }

      println(s"[DATAFRUIT] Task ${taskEnd.taskInfo.taskId} failed (attempt $retryCount): ${classifiedException.description}")
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    metricsCollector.executorsAdded.incrementAndGet()
    println(s"[DATAFRUIT] Executor ${executorAdded.executorId} added")
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    metricsCollector.executorsRemoved.incrementAndGet()
    println(s"[DATAFRUIT] Executor ${executorRemoved.executorId} removed: ${executorRemoved.reason}")
  }

  override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {
    metricsCollector.executorsBlacklisted.incrementAndGet()
    println(s"[DATAFRUIT] Executor ${executorBlacklisted.executorId} blacklisted")
  }

  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {
    metricsCollector.executorsUnblacklisted.incrementAndGet()
    println(s"[DATAFRUIT] Executor ${executorUnblacklisted.executorId} unblacklisted")
  }

  private def detectJobFailurePatterns(jobId: Int, exception: Exception): Unit = {
    val errorPattern = exception.getClass.getSimpleName
    if (exception.getMessage != null && exception.getMessage.toLowerCase.contains("outofmemory")) {
      val issue = PerformanceIssue(
        issueType = "JobMemoryFailure",
        severity = "HIGH",
        description = s"Job $jobId failed due to memory issues",
        recommendation = "Increase executor memory or optimize data processing",
        detectedAt = System.currentTimeMillis(),
        metrics = Map("jobId" -> jobId, "errorType" -> errorPattern)
      )
      metricsCollector.addPerformanceIssue(issue)
    }
  }

  private def analyzeStagePerformance(stageInfo: StageInfo): Unit = {
    Option(stageInfo.taskMetrics).foreach { taskMetrics =>
      metricsCollector.gcTimeTotal.addAndGet(taskMetrics.jvmGCTime)
      metricsCollector.shuffleReadBytes.addAndGet(taskMetrics.shuffleReadMetrics.totalBytesRead)
      metricsCollector.shuffleWriteBytes.addAndGet(taskMetrics.shuffleWriteMetrics.bytesWritten)
      metricsCollector.diskSpillBytes.addAndGet(taskMetrics.diskBytesSpilled)
      metricsCollector.memorySpillBytes.addAndGet(taskMetrics.memoryBytesSpilled)

      val shuffleReadWaitTime = taskMetrics.shuffleReadMetrics.fetchWaitTime
      val shuffleWriteTime = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(taskMetrics.shuffleWriteMetrics.writeTime)
      metricsCollector.ioTimeTotal.addAndGet(shuffleReadWaitTime + shuffleWriteTime)

      detectPerformanceIssues(stageInfo, taskMetrics)
    }
  }

  private def analyzeTaskPerformance(taskEnd: SparkListenerTaskEnd): Unit = {
    Option(taskEnd.taskMetrics).foreach { metrics =>
      val duration = taskEnd.taskInfo.duration
      if (duration > 300000) {
        val issue = PerformanceIssue(
          issueType = "SlowTask",
          severity = "MEDIUM",
          description = s"Task ${taskEnd.taskInfo.taskId} took ${duration / 1000} seconds",
          recommendation = "Investigate data skew or task optimization",
          detectedAt = System.currentTimeMillis(),
          metrics = Map("duration" -> duration, "taskId" -> taskEnd.taskInfo.taskId)
        )
        metricsCollector.addPerformanceIssue(issue)
      }
    }
  }

  private def detectPerformanceIssues(stageInfo: StageInfo, taskMetrics: TaskMetrics): Unit = {
    if (taskMetrics.executorRunTime > 0 && taskMetrics.jvmGCTime.toDouble / taskMetrics.executorRunTime > 0.3) {
      val issue = PerformanceIssue(
        issueType = "ExcessiveGC",
        severity = "HIGH",
        description = s"Stage ${stageInfo.stageId} has excessive GC time (${taskMetrics.jvmGCTime}ms)",
        recommendation = "Increase executor memory or optimize data structures",
        detectedAt = System.currentTimeMillis(),
        metrics = Map("gcTime" -> taskMetrics.jvmGCTime, "executorRunTime" -> taskMetrics.executorRunTime)
      )
      metricsCollector.addPerformanceIssue(issue)
    }

    if (taskMetrics.memoryBytesSpilled > 0) {
      val issue = PerformanceIssue(
        issueType = "MemorySpill",
        severity = "MEDIUM",
        description = s"Stage ${stageInfo.stageId} spilled ${taskMetrics.memoryBytesSpilled} bytes to memory",
        recommendation = "Increase spark.sql.adaptive.coalescePartitions.parallelismFirst or executor memory",
        detectedAt = System.currentTimeMillis(),
        metrics = Map("memorySpilled" -> taskMetrics.memoryBytesSpilled)
      )
      metricsCollector.addPerformanceIssue(issue)
    }

    if (taskMetrics.diskBytesSpilled > 0) {
      val issue = PerformanceIssue(
        issueType = "DiskSpill",
        severity = "HIGH",
        description = s"Stage ${stageInfo.stageId} spilled ${taskMetrics.diskBytesSpilled} bytes to disk",
        recommendation = "Increase executor memory significantly or repartition data",
        detectedAt = System.currentTimeMillis(),
        metrics = Map("diskSpilled" -> taskMetrics.diskBytesSpilled)
      )
      metricsCollector.addPerformanceIssue(issue)
    }

    val shuffleReadBytes = taskMetrics.shuffleReadMetrics.totalBytesRead
    val shuffleWriteBytes = taskMetrics.shuffleWriteMetrics.bytesWritten
    if (shuffleReadBytes > 0 && shuffleWriteBytes > 0) {
      val ratio = shuffleReadBytes.toDouble / shuffleWriteBytes
      if (ratio > 10) {
        val issue = PerformanceIssue(
          issueType = "ShuffleInefficiency",
          severity = "MEDIUM",
          description = s"Stage ${stageInfo.stageId} has inefficient shuffle (read/write ratio: ${ratio})",
          recommendation = "Consider broadcast joins or optimize join strategies",
          detectedAt = System.currentTimeMillis(),
          metrics = Map("shuffleReadBytes" -> shuffleReadBytes, "shuffleWriteBytes" -> shuffleWriteBytes, "ratio" -> ratio)
        )
        metricsCollector.addPerformanceIssue(issue)
      }
    }
  }
}

class DataFruitDriverPlugin extends DriverPlugin with Logging {
  private var sparkContext: SparkContext = _
  private var metricsCollector: SparkMetricsCollector = _
  private var sparkListener: DataFruitSparkListener = _
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def init(sc: SparkContext, ctx: PluginContext): java.util.Map[String, String] = {
    sparkContext = sc
    metricsCollector = new SparkMetricsCollector()
    sparkListener = new DataFruitSparkListener(metricsCollector)
    sc.addSparkListener(sparkListener)
    println("[DATAFRUIT] Driver plugin initialized with comprehensive monitoring")
    startMetricsReporting()
    new java.util.HashMap[String, String]()
  }

  override def receive(message: Any): AnyRef = {
    try {
      message match {
        case msg: DataFruitMessage =>
          msg match {
            case CpuSnapshotMessage(snap) => metricsCollector.addCpuSnapshot(snap)
            case MemorySnapshotMessage(snap) => metricsCollector.addMemorySnapshot(snap)
          }
        case _ => logWarning(s"[DATAFRUIT] Received unknown message type: ${message.getClass.getName}")
      }
    } catch {
      case e: Exception => logError("[DATAFRUIT ERROR] Failed to process message from executor", e)
    }
    null
  }

  private def startMetricsReporting(): Unit = {
    val reportingThread = new Thread(() => {
      while (true) {
        Thread.sleep(60000)
        reportMetrics()
      }
    })
    reportingThread.setDaemon(true)
    reportingThread.start()
  }

  private def reportMetrics(): Unit = {
    val webhookUrl = sparkContext.getConf.get("spark.datafruit.webhook.url", "")
    val apiKey = sparkContext.getConf.get("spark.datafruit.api.key", "your_default_api_key_here")
    val webhookTimeout = sparkContext.getConf.getInt("spark.datafruit.webhook.timeout", 30)
    val webhookRetries = sparkContext.getConf.getInt("spark.datafruit.webhook.retries", 3)

    if (webhookUrl.nonEmpty) {
      sendWebhookWithRetry(webhookUrl, apiKey, webhookTimeout, webhookRetries)
    } else {
      val payload = metricsCollector.getWebhookPayload(apiKey)
      val jsonPayload = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload)
      println(s"[DATAFRUIT METRICS]\n$jsonPayload")
    }
  }

  private def sendWebhookWithRetry(webhookUrl: String, apiKey: String, timeoutSeconds: Int, maxRetries: Int): Unit = {
    var attempt = 0
    var success = false

    while (attempt < maxRetries && !success) {
      attempt += 1
      try {
        val payload = metricsCollector.getWebhookPayload(apiKey)
        val jsonPayload = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload)
        
        val client = HttpClient.newBuilder()
          .connectTimeout(java.time.Duration.ofSeconds(timeoutSeconds))
          .build()

        val request = HttpRequest.newBuilder()
          .uri(URI.create(webhookUrl))
          .header("Content-Type", "application/json")
          .header("User-Agent", "DataFruit-Spark-Monitor/1.0")
          .header("X-API-Key", apiKey)
          .timeout(java.time.Duration.ofSeconds(timeoutSeconds))
          .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
          .build()

        val response = client.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          logInfo(s"[DATAFRUIT] Successfully sent metrics to webhook on attempt $attempt. Status: ${response.statusCode()}")
          success = true
        } else {
          logWarning(s"[DATAFRUIT] Webhook attempt $attempt failed. Status: ${response.statusCode()}, Body: ${response.body()}")
          if (attempt < maxRetries) {
            Thread.sleep(1000 * attempt)
          }
        }
      } catch {
        case e: Exception =>
          logError(s"[DATAFRUIT ERROR] Webhook attempt $attempt failed with exception", e)
          if (attempt < maxRetries) {
            Thread.sleep(1000 * attempt)
          }
      }
    }

    if (!success) {
      logError(s"[DATAFRUIT ERROR] Failed to send webhook after $maxRetries attempts")
    }
  }

  def sendUrgentAlert(alertType: String, message: String, severity: String): Unit = {
    val webhookUrl = sparkContext.getConf.get("spark.datafruit.webhook.url", "")
    val apiKey = sparkContext.getConf.get("spark.datafruit.api.key", "your_default_api_key_here")
    
    if (webhookUrl.nonEmpty) {
      try {
        val urgentPayload = Map(
          "api_key" -> apiKey,
          "alert_type" -> "URGENT",
          "timestamp" -> System.currentTimeMillis(),
          "application_id" -> sparkContext.applicationId,
          "severity" -> severity,
          "alert_category" -> alertType,
          "message" -> message,
          "metrics_snapshot" -> metricsCollector.getMetricsSummary
        )

        val jsonPayload = mapper.writeValueAsString(urgentPayload)
        val client = HttpClient.newHttpClient()
        val request = HttpRequest.newBuilder()
          .uri(URI.create(webhookUrl))
          .header("Content-Type", "application/json")
          .header("X-API-Key", apiKey)
          .header("X-DataFruit-Alert", "true")
          .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
          .build()

        val response = client.send(request, HttpResponse.BodyHandlers.ofString())
        logInfo(s"[DATAFRUIT URGENT] Sent alert '$alertType' with status: ${response.statusCode()}")
      } catch {
        case e: Exception =>
          logError(s"[DATAFRUIT ERROR] Failed to send urgent alert", e)
      }
    }
  }

  override def shutdown(): Unit = {
    if (sparkContext != null && sparkListener != null) {
      sparkContext.removeSparkListener(sparkListener)
    }
    println("[DATAFRUIT] Driver plugin shutdown")
  }
}

class DataFruitExecutorPlugin extends ExecutorPlugin with Logging {
  private var executorId: String = _
  private var pluginContext: PluginContext = _
  private val scheduler = Executors.newSingleThreadScheduledExecutor()

  override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    this.pluginContext = ctx
    this.executorId = ctx.executorID()
    logInfo(s"[DATAFRUIT] Executor plugin initialized on executor $executorId")

    val snapshotTask = new Runnable {
      def run(): Unit = {
        try {
          sendCpuSnapshot()
          sendMemorySnapshot()
        } catch {
          case e: Exception => logError("[DATAFRUIT ERROR] Failed to collect or send snapshot", e)
        }
      }
    }
    scheduler.scheduleAtFixedRate(snapshotTask, 15, 30, TimeUnit.SECONDS)
  }

  private def sendCpuSnapshot(): Unit = {
    val osBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    val snap = CpuSnapshot(
      timestamp = System.currentTimeMillis(),
      executorId = this.executorId,
      processCpuTime = osBean.getProcessCpuTime,
      systemCpuLoad = osBean.getSystemCpuLoad * 100.0,
      processCpuLoad = osBean.getProcessCpuLoad * 100.0,
      availableProcessors = osBean.getAvailableProcessors
    )
    pluginContext.send(CpuSnapshotMessage(snap))
  }

  private def sendMemorySnapshot(): Unit = {
    val memBean = ManagementFactory.getMemoryMXBean
    val osBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    val sparkEnv = SparkEnv.get

    val heap = memBean.getHeapMemoryUsage
    val offHeap = memBean.getNonHeapMemoryUsage

    val snap = MemorySnapshot(
      timestamp = System.currentTimeMillis(),
      executorId = this.executorId,
      heapUsed = heap.getUsed,
      heapCommitted = heap.getCommitted,
      heapMax = heap.getMax,
      offHeapUsed = offHeap.getUsed,
      offHeapCommitted = offHeap.getCommitted,
      storageMemoryUsed = sparkEnv.memoryManager.storageMemoryUsed,
      storageMemoryMax = sparkEnv.memoryManager.maxOnHeapStorageMemory + sparkEnv.memoryManager.maxOffHeapStorageMemory,
      executionMemoryUsed = sparkEnv.memoryManager.executionMemoryUsed,
      executionMemoryMax = heap.getMax - (sparkEnv.memoryManager.maxOnHeapStorageMemory + sparkEnv.memoryManager.maxOffHeapStorageMemory),
      totalSystemMemory = osBean.getTotalPhysicalMemorySize,
      freeSystemMemory = osBean.getFreePhysicalMemorySize
    )
    pluginContext.send(MemorySnapshotMessage(snap))
  }

  override def shutdown(): Unit = {
    scheduler.shutdown()
    logInfo(s"[DATAFRUIT] Executor plugin shutdown on executor $executorId")
  }
}