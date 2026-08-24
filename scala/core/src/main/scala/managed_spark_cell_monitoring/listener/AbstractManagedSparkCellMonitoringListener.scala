// Copyright 2026 Google LLC
// Copyright 2017 CERN
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package managed_spark_cell_monitoring.listener
/** This package provides a custom implementation of a SparkListener interface that forwards data to Jupyter Kernels. */

import org.apache.spark.scheduler._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark._
import managed_spark_cell_monitoring.listener.UIData._
import scala.collection.mutable.{ HashMap, HashSet, ListBuffer }
import java.net._
import java.io._
import org.apache.log4j.Logger
import java.util.{TimerTask,Timer}

/**
 * A SparkListener Implementation that forwards data to a Jupyter Kernel
 *
 *  - All data is forwarded to a jupyter kernel using sockets configured by an environment variable.
 *  - The listener receives notifications of the spark application's events, through the overrided methods.
 *  - The received data is stored and sent as JSON to the kernel socket.
 *  - Overrides methods that correspond to events in a spark Application.
 *  - The argument for each overrided method contains the received data for that event. (See SparkListener docs for more information.)
 *  - For each application, job, stage, and task there is a 'start' and an 'end' event. For executors, there are 'added' and 'removed' events
 *
 *  @constructor called by Spark internally
 *  @param conf Spark configuration object used to start the spark application.
 */
abstract class AbstractManagedSparkCellMonitoringListener(conf: SparkConf) extends SparkListener {

  protected def getStageAttemptNumber(stageInfo: StageInfo): Int
  protected def removeFirstNElements[T](buffer: ListBuffer[T], n: Int): Unit


  val logger = Logger.getLogger(this.getClass.getName)
  val port = scala.util.Properties.envOrElse("SPARK_CELL_MONITOR_KERNEL_PORT", "ERRORNOTFOUND")
  var socket: Socket = null
  var onStageStatusActiveTask: TimerTask = null
  var activeTimer: Timer = null
  var out: OutputStreamWriter = null
  val sparkStageActiveRate: Long = 250L // 250ms

  startConnection()

  /** Send a string message to the kernel using the socket. */
  private val messageQueue = new java.util.concurrent.LinkedBlockingQueue[String]()
  @volatile private var running = true
  private val senderThread = {
    val t = new Thread(new Runnable {
      override def run(): Unit = {
        while (running) {
          try {
            val msg = messageQueue.take()
            synchronized {
              if (out != null) {
                out.write(msg + ";EOD:")
                out.flush()
              }
            }
          } catch {
            case _: InterruptedException => Thread.currentThread().interrupt()
            case _: Throwable => ()
          }
        }
      }
    })
    t.setDaemon(true)
    t.start()
    t
  }

  def send(msg: String): Unit = {
    messageQueue.offer(msg)
  }

  /** Start the socket connection to the kernel and start the send task. The kernel is the server already waiting for connections.*/
  def startConnection(): Unit = {
    if (port == "ERRORNOTFOUND") {
      logger.warn("SPARK_CELL_MONITOR_KERNEL_PORT not found in environment. Managed Spark Cell Monitoring listener will be disabled.")
      return
    }
    try {
      val parsedPort = port.toInt
      socket = new Socket("localhost", parsedPort)
      out = new OutputStreamWriter(socket.getOutputStream(), java.nio.charset.StandardCharsets.UTF_8)

      activeTimer = new Timer(true)

      if (onStageStatusActiveTask == null) {
        onStageStatusActiveTask = new TimerTask {
          def run() = {
            onStageStatusActive()
          }
        }
      }
      activeTimer.schedule(onStageStatusActiveTask, sparkStageActiveRate, sparkStageActiveRate)
    } catch {
      case e: Throwable =>
        logger.error("Failed to start connection to Jupyter kernel", e)
        if (socket != null) {
          try { socket.close() } catch { case _: Throwable => }
          socket = null
        }
        if (out != null) {
          try { out.close() } catch { case _: Throwable => }
          out = null
        }
    }
  }

  /** Close the socket connection to the kernel.*/
  def closeConnection(): Unit = {
    // Wait briefly for the queue to drain before shutting down
    var waitTime = 0
    while (!messageQueue.isEmpty && waitTime < 2000) {
      Thread.sleep(10)
      waitTime += 10
    }
    
    running = false
    if (senderThread != null) {
      try { senderThread.interrupt() } catch { case _: Throwable => }
    }
    messageQueue.clear()

    if (out != null) {
      try { out.close() } catch { case _: Throwable => }
      out = null
    }
    if (socket != null) {
      try { socket.close() } catch { case _: Throwable => }
      socket = null
    }
    if (onStageStatusActiveTask != null) {
      try { onStageStatusActiveTask.cancel() } catch { case _: Throwable => }
      onStageStatusActiveTask = null
    }
    if (activeTimer != null) {
      try { activeTimer.cancel() } catch { case _: Throwable => }
      activeTimer = null
    }
  }

  type JobId = Int
  type JobGroupId = String
  type StageId = Int
  type StageAttemptId = Int

  //Application
  var appId: String = ""

  //Jobs
  val completedJobs = ListBuffer[JobUIData]()
  val failedJobs = ListBuffer[JobUIData]()
  val jobIdToData = new HashMap[JobId, JobUIData]

  // Stages:
  val activeStages = new HashMap[StageId, StageInfo]
  val completedStages = ListBuffer[StageInfo]()
  val skippedStages = ListBuffer[StageInfo]()
  val failedStages = ListBuffer[StageInfo]()
  val stageIdToData = new HashMap[(StageId, StageAttemptId), StageUIData]
  val stageIdToInfo = new HashMap[StageId, StageInfo]
  val stageIdToActiveJobIds = new HashMap[StageId, HashSet[JobId]]


  val retainedStages = conf.getInt("spark.ui.retainedStages", 1000)
  val retainedJobs = conf.getInt("spark.ui.retainedJobs", 1000)

  val executorCores = new HashMap[String, Int]
  @volatile var totalCores: Int = 0
  @volatile var numExecutors: Int = 0

  /**
   * Called when a spark application starts.
   *
   * The application start time and app ID are obtained here.
   */
  override def onApplicationStart(appStarted: SparkListenerApplicationStart): Unit = {
    appId = appStarted.appId.getOrElse("null")
    val json = ("msgtype" -> "sparkApplicationStart") ~
      ("startTime" -> appStarted.time) ~
      ("appId" -> appId) ~
      ("appAttemptId" -> appStarted.appAttemptId.getOrElse("null")) ~
      ("appName" -> appStarted.appName) ~
      ("sparkUser" -> appStarted.sparkUser)

    send(compact(render(json)))
  }

  /**
   * Called when a spark application ends.
   *
   * Closes the socket connection to the kernel.
   */
  override def onApplicationEnd(appEnded: SparkListenerApplicationEnd): Unit = {
    val json = ("msgtype" -> "sparkApplicationEnd") ~
      ("endTime" -> appEnded.time)

    send(compact(render(json)))
    closeConnection()
  }

  /** Converts stageInfo object to a JSON object. */
  def stageInfoToJSON(stageInfo: StageInfo): JObject = {
    val completionTime: Long = stageInfo.completionTime.getOrElse(-1)
    val submissionTime: Long = stageInfo.submissionTime.getOrElse(-1)

    (stageInfo.stageId.toString ->
      ("attemptId" -> getStageAttemptNumber(stageInfo)) ~
      ("name" -> stageInfo.name) ~
      ("numTasks" -> stageInfo.numTasks) ~
      ("completionTime" -> completionTime) ~
      ("submissionTime" -> submissionTime))
  }

  /**
   * Called when a job starts.
   *
   * The jobStart object contains the list of planned stages. They are stored for tracking skipped stages.
   * The total number of tasks is also estimated from the list of planned stages,
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {

    val jobGroup = for (
      props <- Option(jobStart.properties);
      group <- Option(props.getProperty("spark.jobGroup.id"))
    ) yield group

    val jobData: JobUIData =
      new JobUIData(
        jobId = jobStart.jobId,
        stageIds = jobStart.stageIds)

    var stageinfojson: JObject = Nil
    for (x <- jobStart.stageInfos) {
      stageinfojson = stageinfojson ~ stageInfoToJSON(x)
    }
    jobData.numTasks = {
      val allStages = jobStart.stageInfos
      val missingStages = allStages.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }
    jobIdToData(jobStart.jobId) = jobData
    for (stageId <- jobStart.stageIds) {
      stageIdToActiveJobIds.getOrElseUpdate(stageId, new HashSet[JobId]).add(jobStart.jobId)
    }
    // If there's no information for a stage, store the StageInfo received from the scheduler
    // so that we can display stage descriptions for pending stages:
    for (stageInfo <- jobStart.stageInfos) {
      stageIdToInfo.getOrElseUpdate(stageInfo.stageId, stageInfo)
      stageIdToData.getOrElseUpdate((stageInfo.stageId, getStageAttemptNumber(stageInfo)), new StageUIData)
    }
    val name = Option(jobStart.properties).map(_.getProperty("callSite.short", "null")).getOrElse("null")
    val json = ("msgtype" -> "sparkJobStart") ~
      ("jobGroup" -> jobGroup.getOrElse("null")) ~
      ("jobId" -> jobStart.jobId) ~
      ("status" -> "RUNNING") ~
      ("submissionTime" -> Option(jobStart.time).filter(_ >= 0)) ~
      ("stageIds" -> jobStart.stageIds) ~
      ("stageInfos" -> stageinfojson) ~
      ("numTasks" -> jobData.numTasks) ~
      ("totalCores" -> totalCores) ~
      ("appId" -> appId) ~
      ("numExecutors" -> numExecutors) ~
      ("name" -> name)
    send(compact(render(json)))
  }

  /** Called when a job ends. */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    val jobData = jobIdToData.getOrElse(jobEnd.jobId, new JobUIData(jobId = jobEnd.jobId))
    jobData.completionTime = Option(jobEnd.time).filter(_ >= 0)
    var status = "null"
    jobEnd.jobResult match {
      case JobSucceeded =>
        completedJobs += jobData
        trimJobsIfNecessary(completedJobs)
        status = "COMPLETED"
      case _ =>
        failedJobs += jobData
        trimJobsIfNecessary(failedJobs)
        status = "FAILED"
    }
    for (stageId <- jobData.stageIds) {
      stageIdToActiveJobIds.get(stageId).foreach { jobsUsingStage =>
        jobsUsingStage.remove(jobEnd.jobId)
        if (jobsUsingStage.isEmpty) {
          stageIdToActiveJobIds.remove(stageId)
        }
        stageIdToInfo.get(stageId).foreach { stageInfo =>
          if (stageInfo.submissionTime.isEmpty) {
            // if this stage is pending, it won't complete, so mark it as "skipped":
            skippedStages += stageInfo
            trimStagesIfNecessary(skippedStages)

          }
        }
      }
    }

    val json = ("msgtype" -> "sparkJobEnd") ~
      ("jobId" -> jobEnd.jobId) ~
      ("status" -> status) ~
      ("completionTime" -> jobData.completionTime)


    send(compact(render(json)))
  }

  /** Called when a stage is completed. */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    val stage = stageCompleted.stageInfo
    stageIdToInfo(stage.stageId) = stage
    val stageData = stageIdToData.getOrElseUpdate((stage.stageId, getStageAttemptNumber(stage)), {
      new StageUIData
    })
    var status = "UNKNOWN"
    activeStages.remove(stage.stageId)
    if (stage.failureReason.isEmpty) {
      completedStages += stage
      trimStagesIfNecessary(completedStages)
      status = "COMPLETED"
    } else {
      failedStages += stage
      trimStagesIfNecessary(failedStages)
      status = "FAILED"
    }

    val jobIds = stageIdToActiveJobIds.get(stage.stageId)

    val completionTime: Long = stage.completionTime.getOrElse(-1)
    val submissionTime: Long = stage.submissionTime.getOrElse(-1)
    val json = ("msgtype" -> "sparkStageCompleted") ~
      ("stageId" -> stage.stageId) ~
      ("stageAttemptId" -> getStageAttemptNumber(stage)) ~
      ("completionTime" -> completionTime) ~
      ("submissionTime" -> submissionTime) ~
      ("numTasks" -> stage.numTasks) ~
      ("numFailedTasks" -> stageData.numFailedTasks) ~
      ("numCompletedTasks" -> stageData.numCompletedTasks) ~
      ("status" -> status) ~
      ("jobIds" -> jobIds)

    send(compact(render(json)))
  }

  /** Called when a stage is submitted for execution. */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    val stage = stageSubmitted.stageInfo
    activeStages(stage.stageId) = stage
    stageIdToInfo(stage.stageId) = stage
    val stageData = stageIdToData.getOrElseUpdate((stage.stageId, getStageAttemptNumber(stage)), new StageUIData)

    val activeJobsDependentOnStage = stageIdToActiveJobIds.get(stage.stageId)
    val jobIds = activeJobsDependentOnStage
    val submissionTime: Long = stage.submissionTime.getOrElse(-1)
    val json = ("msgtype" -> "sparkStageSubmitted") ~
      ("stageId" -> stage.stageId) ~
      ("stageAttemptId" -> getStageAttemptNumber(stage)) ~
      ("name" -> stage.name) ~
      ("numTasks" -> stage.numTasks) ~
      ("parentIds" -> stage.parentIds) ~
      ("submissionTime" -> submissionTime) ~
      ("jobIds" -> jobIds)
    send(compact(render(json)))
  }

  /** Called when scheduled stage tasks update was requested */
  def onStageStatusActive(): Unit = synchronized {
    // 1. Update on status of active stages
    for ((stageId, stageInfo) <- activeStages) {
      val stageData = stageIdToData.getOrElseUpdate((stageInfo.stageId, getStageAttemptNumber(stageInfo)), new StageUIData)
      val jobIds = stageIdToActiveJobIds.get(stageInfo.stageId)

      val currentActive = stageData.numActiveTasks
      val currentCompleted = stageData.numCompletedTasks
      val currentFailed = stageData.numFailedTasks

      val hasStageChanged = (currentActive != stageData.lastSentActive) ||
                            (currentCompleted != stageData.lastSentCompleted) ||
                            (currentFailed != stageData.lastSentFailed)

      if (hasStageChanged) {
        stageData.lastSentActive = currentActive
        stageData.lastSentCompleted = currentCompleted
        stageData.lastSentFailed = currentFailed

        val json = ("msgtype" -> "sparkStageActive") ~
          ("stageId" -> stageInfo.stageId) ~
          ("stageAttemptId" -> getStageAttemptNumber(stageInfo)) ~
          ("name" -> stageInfo.name) ~
          ("parentIds" -> stageInfo.parentIds) ~
          ("numTasks" -> stageInfo.numTasks) ~
          ("numActiveTasks" -> currentActive) ~
          ("numFailedTasks" -> currentFailed) ~
          ("numCompletedTasks" -> currentCompleted) ~
          ("jobIds" -> jobIds)

        send(compact(render(json)))
      }
    }
  }

  /** Called when a task is started. */
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val taskInfo = taskStart.taskInfo
    if (taskInfo != null && stageIdToInfo.contains(taskStart.stageId)) {
      val stageData = stageIdToData.getOrElseUpdate((taskStart.stageId, taskStart.stageAttemptId), {
        new StageUIData
      })
      stageData.numActiveTasks += 1
    }

  }

  /** Called when a task is ended. */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
    // completion event is for. Let's just drop it here. This means we might have some speculation
    // tasks on the web ui that's never marked as complete.
    if (info != null && taskEnd.stageAttemptId != -1 && stageIdToInfo.contains(taskEnd.stageId)) {
      val stageData = stageIdToData.getOrElseUpdate((taskEnd.stageId, taskEnd.stageAttemptId), {
        new StageUIData
      })
      stageData.numActiveTasks -= 1

      taskEnd.reason match {
        case org.apache.spark.Success =>
          stageData.numCompletedTasks += 1
        case e: ExceptionFailure => // Handle ExceptionFailure because we might have accumUpdates
          stageData.numFailedTasks += 1
        case e: TaskFailedReason => // All other failure cases
          stageData.numFailedTasks += 1
      }
    }
  }

  /** If stored stages data is too large, remove and garbage collect old stages */
  private def trimStagesIfNecessary(stages: ListBuffer[StageInfo]) = synchronized {
    if (stages.size > retainedStages) {
      val toRemove = calculateNumberToRemove(stages.size, retainedStages)
      stages.take(toRemove).foreach { s =>
        stageIdToData.remove((s.stageId, getStageAttemptNumber(s)))
        stageIdToInfo.remove(s.stageId)
      }
      removeFirstNElements(stages, toRemove)
    }
  }

  /** If stored jobs data is too large, remove and garbage collect old jobs */
  private def trimJobsIfNecessary(jobs: ListBuffer[JobUIData]) = synchronized {
    if (jobs.size > retainedJobs) {
      val toRemove = calculateNumberToRemove(jobs.size, retainedJobs)
      jobs.take(toRemove).foreach { job =>
        // Remove the job's UI data, if it exists
        jobIdToData.remove(job.jobId)
      }
      removeFirstNElements(jobs, toRemove)
    }
  }

  /** Calculate number of items to remove from stored data. */
  private def calculateNumberToRemove(dataSize: Int, retainedSize: Int): Int = {
    math.max(retainedSize / 10, dataSize - retainedSize)
  }

  /** Called when an executor is added. */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    executorCores(executorAdded.executorId) = executorAdded.executorInfo.totalCores
    totalCores += executorAdded.executorInfo.totalCores
    numExecutors += 1
    val json = ("msgtype" -> "sparkExecutorAdded") ~
      ("executorId" -> executorAdded.executorId) ~
      ("time" -> executorAdded.time) ~
      ("host" -> executorAdded.executorInfo.executorHost) ~
      ("numCores" -> executorAdded.executorInfo.totalCores) ~
      ("totalCores" -> totalCores) // Sending this as browser data can be lost during reloads

    send(compact(render(json)))
  }

  /** Called when an executor is removed. */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    totalCores -= executorCores.getOrElse(executorRemoved.executorId, 0)
    numExecutors -= 1
    val json = ("msgtype" -> "sparkExecutorRemoved") ~
      ("executorId" -> executorRemoved.executorId) ~
      ("time" -> executorRemoved.time) ~
      ("totalCores" -> totalCores) // Sending this as browser data can be lost during reloads


    send(compact(render(json)))
  }
}

/** Data Structures for storing received from listener events. */
object UIData {

  /**
   * Data about a job.
   *
   * This is stored to track aggregated valus such as number of stages and tasks
   */
  class JobUIData(
    var jobId: Int = -1,
    var completionTime: Option[Long] = None,
    var stageIds: Seq[Int] = Seq.empty,
    var numTasks: Int = 0)

  /**
   * Data about a stage.
   *
   * This is stored to track aggregated valus such as number of tasks.
   */
  class StageUIData {
    var numActiveTasks: Int = _
    var numCompletedTasks: Int = _
    var numFailedTasks: Int = _

    // State-change tracking
    var lastSentActive: Int = -1
    var lastSentCompleted: Int = -1
    var lastSentFailed: Int = -1
  }
}
