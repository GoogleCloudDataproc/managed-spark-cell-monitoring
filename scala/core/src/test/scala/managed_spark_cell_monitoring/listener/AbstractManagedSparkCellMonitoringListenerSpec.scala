// Copyright 2026 Google LLC
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import scala.collection.mutable.ListBuffer
import java.net.{ServerSocket, InetAddress}
import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

class AbstractManagedSparkCellMonitoringListenerSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  class TestListener(conf: SparkConf) extends AbstractManagedSparkCellMonitoringListener(conf) {
    override protected def getStageAttemptNumber(stageInfo: StageInfo): Int = 0
    override protected def removeFirstNElements[T](buffer: ListBuffer[T], n: Int): Unit = {
      buffer.remove(0, math.min(n, buffer.size))
    }
  }

  "AbstractManagedSparkCellMonitoringListener" should "fail-open if SPARK_CELL_MONITOR_KERNEL_PORT is missing" in {
    val conf = new SparkConf()
    val listener = new TestListener(conf)
    
    listener.socket shouldBe null
    listener.out shouldBe null
    noException should be thrownBy listener.send("test message")
  }

  it should "gracefully handle exceptions inside closeConnection" in {
    val conf = new SparkConf()
    val listener = new TestListener(conf)
    noException should be thrownBy listener.closeConnection()
  }

  def withListenerAndSocket(testCode: (TestListener, ServerSocket) => Any): Unit = {
    val server = new ServerSocket(0, 50, InetAddress.getByName("localhost"))
    server.setSoTimeout(5000)
    val portString = server.getLocalPort.toString

    class EnvTestListener(conf: SparkConf) extends TestListener(conf) {
      override val port: String = portString
      startConnection()
    }

    val conf = new SparkConf()
    val listener = new EnvTestListener(conf)
    
    try {
      testCode(listener, server)
    } finally {
      listener.closeConnection()
      server.close()
    }
  }

  def readFromSocket(server: ServerSocket): String = {
    val socket = server.accept()
    socket.setSoTimeout(5000) // PREVENT HANGING ON READ
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    val sb = new StringBuilder()
    var continue = true
    while (continue) {
      val c = in.read()
      if (c == -1) continue = false
      else {
        sb.append(c.toChar)
        if (sb.toString().endsWith(";EOD:")) continue = false
      }
    }
    sb.toString()
  }

  it should "successfully connect and send JSON when port is provided" in {
    withListenerAndSocket { (listener, server) =>
      listener.socket shouldNot be (null)
      
      val appStart = SparkListenerApplicationStart("test-app", Some("app-123"), 123456789L, "test-user", None)
      listener.onApplicationStart(appStart)

      val received = readFromSocket(server)
      received should include ("sparkApplicationStart")
      received should include ("test-app")
      received should include (";EOD:")
    }
  }

  it should "handle onJobStart" in {
    withListenerAndSocket { (listener, server) =>
      val stageInfo = mock[StageInfo]
      when(stageInfo.stageId).thenReturn(1)
      when(stageInfo.completionTime).thenReturn(Some(1000L))
      when(stageInfo.submissionTime).thenReturn(Some(500L))
      when(stageInfo.name).thenReturn("stage1")
      when(stageInfo.numTasks).thenReturn(5)
      
      val jobStart = SparkListenerJobStart(1, 1000L, Seq(stageInfo), new Properties())
      listener.onJobStart(jobStart)
      
      // Keep reading until we get the jobStart message to ignore background active task messages
      var received = ""
      while (!received.contains("sparkJobStart")) {
        received = readFromSocket(server)
      }
      received should include ("jobId\":1")
    }
  }

  it should "handle onJobEnd" in {
    withListenerAndSocket { (listener, server) =>
      val jobEnd = SparkListenerJobEnd(1, 1000L, JobSucceeded)
      listener.onJobEnd(jobEnd)
      
      var received = ""
      while (!received.contains("sparkJobEnd")) {
        received = readFromSocket(server)
      }
      received should include ("jobId\":1")
      received should include ("COMPLETED")
    }
  }

  it should "handle onStageSubmitted" in {
    withListenerAndSocket { (listener, server) =>
      val stageInfo = mock[StageInfo]
      when(stageInfo.stageId).thenReturn(1)
      when(stageInfo.completionTime).thenReturn(Some(1000L))
      when(stageInfo.submissionTime).thenReturn(Some(500L))
      when(stageInfo.name).thenReturn("stage1")
      when(stageInfo.numTasks).thenReturn(5)
      when(stageInfo.parentIds).thenReturn(Seq.empty)
      
      val stageSubmitted = SparkListenerStageSubmitted(stageInfo, new Properties())
      listener.onStageSubmitted(stageSubmitted)
      
      var received = ""
      while (!received.contains("sparkStageSubmitted")) {
        received = readFromSocket(server)
      }
      received should include ("stageId\":1")
    }
  }

  it should "handle onStageCompleted" in {
    withListenerAndSocket { (listener, server) =>
      val stageInfo = mock[StageInfo]
      when(stageInfo.stageId).thenReturn(1)
      when(stageInfo.completionTime).thenReturn(Some(1000L))
      when(stageInfo.submissionTime).thenReturn(Some(500L))
      when(stageInfo.name).thenReturn("stage1")
      when(stageInfo.numTasks).thenReturn(5)
      when(stageInfo.failureReason).thenReturn(None)
      
      val stageCompleted = SparkListenerStageCompleted(stageInfo)
      listener.onStageCompleted(stageCompleted)
      
      var received = ""
      while (!received.contains("sparkStageCompleted")) {
        received = readFromSocket(server)
      }
      received should include ("stageId\":1")
    }
  }

  it should "handle onTaskStart" in {
    withListenerAndSocket { (listener, server) =>
      val taskInfo = mock[TaskInfo]
      when(taskInfo.taskId).thenReturn(1L)
      when(taskInfo.index).thenReturn(0)
      when(taskInfo.host).thenReturn("localhost")
      when(taskInfo.executorId).thenReturn("exec1")
      val taskStart = SparkListenerTaskStart(1, 0, taskInfo)
      listener.onTaskStart(taskStart)
      
      // taskStart doesn't emit data directly (only updates state), so we just assert no error
      listener.socket shouldNot be (null)
    }
  }

  it should "handle onTaskEnd" in {
    withListenerAndSocket { (listener, server) =>
      val taskInfo = mock[TaskInfo]
      when(taskInfo.taskId).thenReturn(1L)
      when(taskInfo.index).thenReturn(0)
      when(taskInfo.host).thenReturn("localhost")
      when(taskInfo.executorId).thenReturn("exec1")
      
      val taskEnd = mock[SparkListenerTaskEnd]
      when(taskEnd.stageId).thenReturn(1)
      when(taskEnd.stageAttemptId).thenReturn(0)
      when(taskEnd.taskType).thenReturn("taskType")
      when(taskEnd.reason).thenReturn(org.apache.spark.Success)
      when(taskEnd.taskInfo).thenReturn(taskInfo)
      
      listener.onTaskEnd(taskEnd)
      
      // taskEnd doesn't emit data directly, it just updates internal state!
      listener.socket shouldNot be (null)
    }
  }


  it should "handle onApplicationEnd" in {
    withListenerAndSocket { (listener, server) =>
      val appEnd = SparkListenerApplicationEnd(1000L)
      listener.onApplicationEnd(appEnd)
      
      var received = ""
      while (!received.contains("sparkApplicationEnd")) {
        received = readFromSocket(server)
      }
      received should include ("endTime\":1000")
    }
  }

  it should "handle onExecutorAdded" in {
    withListenerAndSocket { (listener, server) =>
      val execInfo = new org.apache.spark.scheduler.cluster.ExecutorInfo("localhost", 4, Map.empty)
      val execAdded = SparkListenerExecutorAdded(1000L, "exec1", execInfo)
      listener.onExecutorAdded(execAdded)
      
      var received = ""
      while (!received.contains("sparkExecutorAdded")) {
        received = readFromSocket(server)
      }
      received should include ("executorId\":\"exec1")
      received should include ("numCores\":4")
    }
  }

  it should "handle onExecutorRemoved" in {
    withListenerAndSocket { (listener, server) =>
      val execRemoved = SparkListenerExecutorRemoved(1000L, "exec1", "reason")
      listener.onExecutorRemoved(execRemoved)
      
      var received = ""
      while (!received.contains("sparkExecutorRemoved")) {
        received = readFromSocket(server)
      }
      received should include ("executorId\":\"exec1")
    }
  }

  it should "handle onTaskEnd ExceptionFailure" in {
    withListenerAndSocket { (listener, server) =>
      val taskInfo = mock[TaskInfo]
      when(taskInfo.taskId).thenReturn(2L)
      val exceptionFailure = mock[org.apache.spark.ExceptionFailure]
      
      val taskEnd = mock[SparkListenerTaskEnd]
      when(taskEnd.stageId).thenReturn(2)
      when(taskEnd.stageAttemptId).thenReturn(0)
      when(taskEnd.reason).thenReturn(exceptionFailure)
      when(taskEnd.taskInfo).thenReturn(taskInfo)
      
      listener.onTaskEnd(taskEnd)
      listener.socket shouldNot be (null)
    }
  }

  it should "handle onStageStatusActive directly" in {
    withListenerAndSocket { (listener, server) =>
      val stageInfo = mock[StageInfo]
      when(stageInfo.stageId).thenReturn(5)
      when(stageInfo.name).thenReturn("active-stage")
      when(stageInfo.parentIds).thenReturn(Seq.empty)
      when(stageInfo.numTasks).thenReturn(10)

      listener.activeStages(5) = stageInfo
      listener.onStageStatusActive()
      
      var received = ""
      while (!received.contains("sparkStageActive")) {
        received = readFromSocket(server)
      }
      received should include ("active-stage")
    }
  }
}
