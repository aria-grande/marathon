package mesosphere.marathon
package tasks

import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.instance.{ Instance, TestInstanceBuilder }
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.leadership.AlwaysElectedLeadershipModule
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.tracker.{ InstanceTracker, TaskStateOpProcessor }
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.PathId
import mesosphere.marathon.state.PathId.StringPathId
import mesosphere.marathon.storage.repository.legacy.TaskEntityRepository
import mesosphere.marathon.storage.repository.legacy.store.{ InMemoryStore, PersistentStore }
import mesosphere.marathon.test.{ MarathonActorSupport, MarathonShutdownHookSupport, MarathonSpec, MarathonTestHelper }
import mesosphere.mesos.protos.Implicits._
import mesosphere.mesos.protos.TextAttribute
import org.apache.mesos.Protos
import org.apache.mesos.Protos.{ TaskState, TaskStatus }
import org.mockito.Matchers.any
import org.mockito.Mockito.{ reset, spy, times, verify }
import org.scalatest.{ GivenWhenThen, Matchers }

import scala.collection.immutable.Seq

class InstanceTrackerImplTest extends MarathonSpec with MarathonActorSupport
    with Matchers with GivenWhenThen with MarathonShutdownHookSupport {

  import scala.concurrent.ExecutionContext.Implicits.global

  val TEST_APP_NAME = PathId("/foo")
  var instanceTracker: InstanceTracker = null
  var stateOpProcessor: TaskStateOpProcessor = null
  var state: PersistentStore = null
  val config = MarathonTestHelper.defaultConfig()
  val metrics = new Metrics(new MetricRegistry)
  val clock = ConstantClock()

  before {
    state = spy(new InMemoryStore)
    val taskTrackerModule = MarathonTestHelper.createTaskTrackerModule(AlwaysElectedLeadershipModule(shutdownHooks), state, metrics)
    instanceTracker = taskTrackerModule.instanceTracker
    stateOpProcessor = taskTrackerModule.stateOpProcessor
  }

  test("SerializeAndDeserialize") {
    val sampleTask = makeSampleInstance(TEST_APP_NAME)
    val originalInstance: Instance = sampleTask

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(originalInstance)).futureValue

    val deserializedInstance = instanceTracker.instance(originalInstance.instanceId).futureValue

    deserializedInstance should not be empty
    deserializedInstance should equal(Some(originalInstance))
  }

  test("List") {
    testList(_.instancesBySpecSync)
  }

  test("List Async") {
    testList(_.instancesBySpec().futureValue)
  }

  private[this] def testList(call: InstanceTracker => InstanceTracker.InstancesBySpec): Unit = {
    val instance1 = makeSampleInstance(TEST_APP_NAME / "a")
    val instance2 = makeSampleInstance(TEST_APP_NAME / "b")
    val instance3 = makeSampleInstance(TEST_APP_NAME / "b")

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(instance1)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(instance2)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(instance3)).futureValue

    val testAppTasks = call(instanceTracker)

    testAppTasks.allSpecIdsWithInstances should be(Set(TEST_APP_NAME / "a", TEST_APP_NAME / "b"))

    testAppTasks.instancesMap(TEST_APP_NAME / "a").specId should equal(TEST_APP_NAME / "a")
    testAppTasks.instancesMap(TEST_APP_NAME / "b").specId should equal(TEST_APP_NAME / "b")
    testAppTasks.instancesMap(TEST_APP_NAME / "a").instances should have size 1
    testAppTasks.instancesMap(TEST_APP_NAME / "b").instances should have size 2
    testAppTasks.instancesMap(TEST_APP_NAME / "a").instanceMap.keySet should equal(Set(instance1.instanceId))
    testAppTasks.instancesMap(TEST_APP_NAME / "b").instanceMap.keySet should equal(Set(instance2.instanceId, instance3.instanceId))
  }

  test("GetTasks") {
    testGetTasks(_.specInstancesSync(TEST_APP_NAME))
  }

  test("GetTasks Async") {
    testGetTasks(_.specInstances(TEST_APP_NAME).futureValue)
  }

  private[this] def testGetTasks(call: InstanceTracker => Seq[Instance]): Unit = {
    val task1 = makeSampleInstance(TEST_APP_NAME)
    val task2 = makeSampleInstance(TEST_APP_NAME)
    val task3 = makeSampleInstance(TEST_APP_NAME)

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(task1)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(task2)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(task3)).futureValue

    val testAppTasks = call(instanceTracker)

    shouldContainTask(testAppTasks, task1)
    shouldContainTask(testAppTasks, task2)
    shouldContainTask(testAppTasks, task3)
    assert(testAppTasks.size == 3)
  }

  test("Count") {
    testCount(_.countLaunchedSpecInstancesSync(_))
  }

  test("Count Async") {
    testCount(_.countSpecInstances(_).futureValue)
  }

  private[this] def testCount(count: (InstanceTracker, PathId) => Int): Unit = {
    val task1 = makeSampleInstance(TEST_APP_NAME / "a")

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(task1)).futureValue

    count(instanceTracker, TEST_APP_NAME / "a") should be(1)
    count(instanceTracker, TEST_APP_NAME / "b") should be(0)
  }

  test("Contains") {
    testContains(_.hasSpecInstancesSync(_))
  }

  test("Contains Async") {
    testContains(_.hasSpecInstances(_).futureValue)
  }

  private[this] def testContains(count: (InstanceTracker, PathId) => Boolean): Unit = {
    val task1 = makeSampleInstance(TEST_APP_NAME / "a")

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(task1)).futureValue

    count(instanceTracker, TEST_APP_NAME / "a") should be(true)
    count(instanceTracker, TEST_APP_NAME / "b") should be(false)
  }

  test("TaskLifecycle") {
    val sampleInstance = TestInstanceBuilder.newBuilder(TEST_APP_NAME).addTaskStarting().getInstance()

    // CREATE TASK
    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(sampleInstance)).futureValue

    shouldContainTask(instanceTracker.specInstancesSync(TEST_APP_NAME), sampleInstance)
    stateShouldContainKey(state, sampleInstance.instanceId)

    // TASK STATUS UPDATE
    val startingTaskStatus = InstanceUpdateOperation.MesosUpdate(sampleInstance, makeTaskStatus(sampleInstance, TaskState.TASK_STARTING), clock.now())

    stateOpProcessor.process(startingTaskStatus).futureValue

    shouldContainTask(instanceTracker.specInstancesSync(TEST_APP_NAME), sampleInstance)
    stateShouldContainKey(state, sampleInstance.instanceId)
    instanceTracker.specInstancesSync(TEST_APP_NAME).foreach(task => shouldHaveTaskStatus(task, startingTaskStatus))

    // TASK RUNNING
    val runningTaskStatus = InstanceUpdateOperation.MesosUpdate(sampleInstance, makeTaskStatus(sampleInstance, TaskState.TASK_RUNNING), clock.now())

    stateOpProcessor.process(runningTaskStatus).futureValue

    shouldContainTask(instanceTracker.specInstancesSync(TEST_APP_NAME), sampleInstance)
    stateShouldContainKey(state, sampleInstance.instanceId)
    instanceTracker.specInstancesSync(TEST_APP_NAME).foreach(task => shouldHaveTaskStatus(task, runningTaskStatus))

    // TASK STILL RUNNING
    val updatedRunningTaskStatus = InstanceUpdateOperation.MesosUpdate(sampleInstance, makeTaskStatus(sampleInstance, TaskState.TASK_RUNNING), clock.now())
    stateOpProcessor.process(updatedRunningTaskStatus).futureValue
    shouldContainTask(instanceTracker.specInstancesSync(TEST_APP_NAME), sampleInstance)
    instanceTracker.specInstancesSync(TEST_APP_NAME).headOption.foreach(task =>
      shouldHaveTaskStatus(task, runningTaskStatus))

    // TASK TERMINATED
    stateOpProcessor.process(InstanceUpdateOperation.ForceExpunge(sampleInstance.instanceId)).futureValue
    stateShouldNotContainKey(state, sampleInstance.instanceId)

    // APP SHUTDOWN
    assert(!instanceTracker.hasSpecInstancesSync(TEST_APP_NAME), "App was not removed")

    // ERRONEOUS MESSAGE, TASK DOES NOT EXIST ANYMORE
    val erroneousStatus = InstanceUpdateOperation.MesosUpdate(sampleInstance, makeTaskStatus(sampleInstance, TaskState.TASK_LOST), clock.now())

    val failure = stateOpProcessor.process(erroneousStatus).failed.futureValue
    assert(failure.getCause != null)
    assert(failure.getCause.getMessage.contains("does not exist"), s"message: ${failure.getMessage}")
  }

  test("TASK_FAILED status update will expunge task") { testStatusUpdateForTerminalState(TaskState.TASK_FAILED) }
  test("TASK_FINISHED status update will expunge task") { testStatusUpdateForTerminalState(TaskState.TASK_FINISHED) }
  test("TASK_LOST status update will expunge task") { testStatusUpdateForTerminalState(TaskState.TASK_LOST) }
  test("TASK_KILLED status update will expunge task") { testStatusUpdateForTerminalState(TaskState.TASK_KILLED) }
  test("TASK_ERROR status update will expunge task") { testStatusUpdateForTerminalState(TaskState.TASK_ERROR) }

  private[this] def testStatusUpdateForTerminalState(taskState: TaskState): Unit = {
    val sampleTask = makeSampleInstance(TEST_APP_NAME)
    val terminalStatusUpdate = InstanceUpdateOperation.MesosUpdate(sampleTask, makeTaskStatus(sampleTask, taskState), clock.now())

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(sampleTask)).futureValue
    shouldContainTask(instanceTracker.specInstancesSync(TEST_APP_NAME), sampleTask)
    stateShouldContainKey(state, sampleTask.instanceId)

    stateOpProcessor.process(terminalStatusUpdate).futureValue

    shouldNotContainTask(instanceTracker.specInstancesSync(TEST_APP_NAME), sampleTask)
    stateShouldNotContainKey(state, sampleTask.instanceId)
  }

  test("UnknownTasks") {
    val sampleTask = makeSampleInstance(TEST_APP_NAME)

    // don't call taskTracker.created, but directly running
    val runningTaskStatus = InstanceUpdateOperation.MesosUpdate(sampleTask, makeTaskStatus(sampleTask, TaskState.TASK_RUNNING), clock.now())
    val res = stateOpProcessor.process(runningTaskStatus)
    res.failed.futureValue.getCause.getMessage should equal(s"${sampleTask.instanceId} of app [/foo] does not exist")

    shouldNotContainTask(instanceTracker.specInstancesSync(TEST_APP_NAME), sampleTask)
    stateShouldNotContainKey(state, sampleTask.instanceId)
  }

  test("MultipleApps") {
    val appName1 = "app1".toRootPath
    val appName2 = "app2".toRootPath
    val appName3 = "app3".toRootPath

    val app1_task1 = makeSampleInstance(appName1)
    val app1_task2 = makeSampleInstance(appName1)
    val app2_task1 = makeSampleInstance(appName2)
    val app3_task1 = makeSampleInstance(appName3)
    val app3_task2 = makeSampleInstance(appName3)
    val app3_task3 = makeSampleInstance(appName3)

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(app1_task1)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.MesosUpdate(app1_task1, makeTaskStatus(app1_task1, TaskState.TASK_RUNNING), clock.now())).futureValue

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(app1_task2)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.MesosUpdate(app1_task2, makeTaskStatus(app1_task2, TaskState.TASK_RUNNING), clock.now())).futureValue

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(app2_task1)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.MesosUpdate(app2_task1, makeTaskStatus(app2_task1, TaskState.TASK_RUNNING), clock.now())).futureValue

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(app3_task1)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.MesosUpdate(app3_task1, makeTaskStatus(app3_task1, TaskState.TASK_RUNNING), clock.now())).futureValue

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(app3_task2)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.MesosUpdate(app3_task2, makeTaskStatus(app3_task2, TaskState.TASK_RUNNING), clock.now())).futureValue

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(app3_task3)).futureValue
    stateOpProcessor.process(InstanceUpdateOperation.MesosUpdate(app3_task3, makeTaskStatus(app3_task3, TaskState.TASK_RUNNING), clock.now())).futureValue

    assert(state.allIds().futureValue.size == 6, "Incorrect number of tasks in state")

    val app1Tasks = instanceTracker.specInstancesSync(appName1)

    shouldContainTask(app1Tasks, app1_task1)
    shouldContainTask(app1Tasks, app1_task2)
    assert(app1Tasks.size == 2, "Incorrect number of tasks")

    val app2Tasks = instanceTracker.specInstancesSync(appName2)

    shouldContainTask(app2Tasks, app2_task1)
    assert(app2Tasks.size == 1, "Incorrect number of tasks")

    val app3Tasks = instanceTracker.specInstancesSync(appName3)

    shouldContainTask(app3Tasks, app3_task1)
    shouldContainTask(app3Tasks, app3_task2)
    shouldContainTask(app3Tasks, app3_task3)
    assert(app3Tasks.size == 3, "Incorrect number of tasks")
  }

  test("Should not store if state did not change (no health present)") {
    val sampleInstance = makeSampleInstance(TEST_APP_NAME)
    val (_, task) = sampleInstance.tasksMap.head
    val status = task.status.mesosStatus.get
      .toBuilder
      .setTimestamp(123)
      .build()
    val update = InstanceUpdateOperation.MesosUpdate(sampleInstance, status, clock.now())

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(sampleInstance)).futureValue
    stateOpProcessor.process(update).futureValue

    stateOpProcessor.process(update).futureValue

    reset(state)

    stateOpProcessor.process(update).futureValue

    verify(state, times(0)).update(any())
  }

  test("Should not store if state and health did not change") {
    val sampleInstance = TestInstanceBuilder.newBuilder(TEST_APP_NAME).addTaskWithBuilder().taskRunning().asHealthyTask().build().getInstance()
    val (_, task) = sampleInstance.tasksMap.head
    val status = task.status.mesosStatus.get
      .toBuilder
      .setTimestamp(123)
      .build()
    val update = InstanceUpdateOperation.MesosUpdate(sampleInstance, status, clock.now())

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(sampleInstance)).futureValue
    stateOpProcessor.process(update).futureValue

    stateOpProcessor.process(update).futureValue

    reset(state)

    stateOpProcessor.process(update).futureValue

    verify(state, times(0)).update(any())
  }

  test("Should store if state changed") {
    val sampleInstance = TestInstanceBuilder.newBuilder(TEST_APP_NAME).addTaskStaged().getInstance()
    val (_, task) = sampleInstance.tasksMap.head
    val status = task.status.mesosStatus.get.toBuilder
      .setState(Protos.TaskState.TASK_RUNNING)
      .build()
    val update = InstanceUpdateOperation.MesosUpdate(sampleInstance, status, clock.now())

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(sampleInstance)).futureValue
    stateOpProcessor.process(update).futureValue

    stateOpProcessor.process(update).futureValue

    reset(state)

    val newStatus = status.toBuilder
      .setState(Protos.TaskState.TASK_FAILED)
      .build()
    val newUpdate = InstanceUpdateOperation.MesosUpdate(sampleInstance, newStatus, clock.now())

    stateOpProcessor.process(newUpdate).futureValue

    verify(state, times(1)).delete(any())
  }

  test("Should store if health changed") {
    val sampleInstance = TestInstanceBuilder.newBuilder(TEST_APP_NAME).addTaskRunning().getInstance()
    val (_, task) = sampleInstance.tasksMap.head
    val status = task.status.mesosStatus.get.toBuilder
      .setHealthy(true)
      .build()
    val update = InstanceUpdateOperation.MesosUpdate(sampleInstance, status, clock.now())

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(sampleInstance)).futureValue
    stateOpProcessor.process(update).futureValue

    stateOpProcessor.process(update).futureValue

    reset(state)

    val newStatus = status.toBuilder
      .setHealthy(false)
      .build()
    val newUpdate = InstanceUpdateOperation.MesosUpdate(sampleInstance, newStatus, clock.now())

    stateOpProcessor.process(newUpdate).futureValue

    verify(state, times(1)).update(any())
  }

  test("Should store if state and health changed") {
    val sampleInstance = makeSampleInstance(TEST_APP_NAME)
    val status = Protos.TaskStatus
      .newBuilder
      .setState(Protos.TaskState.TASK_RUNNING)
      .setTaskId(Task.Id.forInstanceId(sampleInstance.instanceId, None).mesosTaskId)
      .setHealthy(true)
      .build()
    val update = InstanceUpdateOperation.MesosUpdate(sampleInstance, status, clock.now())

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(sampleInstance)).futureValue
    stateOpProcessor.process(update).futureValue

    stateOpProcessor.process(update).futureValue

    reset(state)

    val newStatus = status.toBuilder
      .setState(Protos.TaskState.TASK_RUNNING)
      .setHealthy(false)
      .build()
    val newUpdate = InstanceUpdateOperation.MesosUpdate(sampleInstance, newStatus, clock.now())

    stateOpProcessor.process(newUpdate).futureValue

    verify(state, times(1)).update(any())
  }

  test("Should store if health changed (no health present at first)") {
    val sampleInstance = makeSampleInstance(TEST_APP_NAME)
    val status = Protos.TaskStatus
      .newBuilder
      .setState(Protos.TaskState.TASK_RUNNING)
      .setTaskId(Task.Id.forInstanceId(sampleInstance.instanceId, None).mesosTaskId)
      .build()
    val update = InstanceUpdateOperation.MesosUpdate(sampleInstance, status, clock.now())

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(sampleInstance)).futureValue
    stateOpProcessor.process(update).futureValue

    stateOpProcessor.process(update).futureValue

    reset(state)

    val newStatus = status.toBuilder
      .setHealthy(true)
      .build()
    val newUpdate = InstanceUpdateOperation.MesosUpdate(sampleInstance, newStatus, clock.now())

    stateOpProcessor.process(newUpdate).futureValue

    verify(state, times(1)).update(any())
  }

  test("Should store if state and health changed (no health present at first)") {
    val sampleInstance = makeSampleInstance(TEST_APP_NAME)
    val status = Protos.TaskStatus
      .newBuilder
      .setState(Protos.TaskState.TASK_RUNNING)
      .setTaskId(Task.Id.forInstanceId(sampleInstance.instanceId, None).mesosTaskId)
      .build()
    val update = InstanceUpdateOperation.MesosUpdate(sampleInstance, status, clock.now())

    stateOpProcessor.process(InstanceUpdateOperation.LaunchEphemeral(sampleInstance)).futureValue
    stateOpProcessor.process(update).futureValue

    stateOpProcessor.process(update).futureValue

    reset(state)

    val newStatus = status.toBuilder
      .setState(Protos.TaskState.TASK_RUNNING)
      .setHealthy(false)
      .build()
    val newUpdate = InstanceUpdateOperation.MesosUpdate(sampleInstance, newStatus, clock.now())

    stateOpProcessor.process(newUpdate).futureValue

    verify(state, times(1)).update(any())
  }

  def makeSampleInstance(appId: PathId): Instance = {
    TestInstanceBuilder.newBuilder(appId).addTaskWithBuilder().taskStaged()
      .withAgentInfo(_.copy(host = "host", attributes = Seq(TextAttribute("attr1", "bar"))))
      .withHostPorts(Seq(999)).build().getInstance()
  }

  def makeTaskStatus(instance: Instance, state: TaskState = TaskState.TASK_RUNNING) = {
    TaskStatus.newBuilder
      .setTaskId(Task.Id.forInstanceId(instance.instanceId, None).mesosTaskId)
      .setState(state)
      .build
  }

  def containsTask(tasks: Seq[Instance], task: Instance) =
    tasks.exists(t => t.instanceId == task.instanceId
      && t.agentInfo.host == task.agentInfo.host
      && t.tasksMap.values.flatMap(_.launched.map(_.hostPorts)) == task.tasksMap.values.flatMap(_.launched.map(_.hostPorts)))
  def shouldContainTask(tasks: Seq[Instance], task: Instance) =
    assert(containsTask(tasks, task), s"Should contain ${task.instanceId}")
  def shouldNotContainTask(tasks: Seq[Instance], task: Instance) =
    assert(!containsTask(tasks, task), s"Should not contain ${task.instanceId}")

  def shouldHaveTaskStatus(task: Instance, stateOp: InstanceUpdateOperation.MesosUpdate): Unit = {
    assert(Option(stateOp.mesosStatus).isDefined, "mesos status is None")
    assert(task.isLaunched)
    assert(
      task.tasksMap.values.map(_.status.mesosStatus.get).forall(status => status == stateOp.mesosStatus),
      s"Should have task status ${stateOp.mesosStatus}")
  }

  def stateShouldNotContainKey(state: PersistentStore, key: Instance.Id): Unit = {
    val keyWithPrefix = TaskEntityRepository.storePrefix + key.idString
    assert(!state.allIds().futureValue.toSet.contains(keyWithPrefix), s"Key $keyWithPrefix was found in state")
  }

  def stateShouldContainKey(state: PersistentStore, key: Instance.Id): Unit = {
    val keyWithPrefix = TaskEntityRepository.storePrefix + key.idString
    assert(state.allIds().futureValue.toSet.contains(keyWithPrefix), s"Key $keyWithPrefix was not found in state")
  }
}
