package mesosphere.marathon
package raml

import mesosphere.marathon.Protos.ResidencyDefinition
import mesosphere.marathon.core.health.{ HealthCheck => CoreHealthCheck, _ }
import mesosphere.marathon.core.readiness.{ ReadinessCheck => CoreReadinessCheck }
import mesosphere.marathon.state._
import org.apache.mesos.{ Protos => mesos }

import scala.concurrent.duration._

trait AppConversion extends EnvVarConversion with NetworkConversion with SecretConversion with ConstraintConversion {
  import AppConversion._

  def resources(cpus: Option[Double], mem: Option[Double], disk: Option[Double], gpus: Option[Int]): Resources =
    Resources(
      cpus = cpus.getOrElse(AppDefinition.DefaultCpus),
      mem = mem.getOrElse(AppDefinition.DefaultMem),
      disk = disk.getOrElse(AppDefinition.DefaultDisk),
      gpus = gpus.getOrElse(AppDefinition.DefaultGpus)
    )

  implicit val residencyRamlReader = Reads[AppResidency, Residency] { residency =>
    import ResidencyDefinition.TaskLostBehavior._
    // TODO(jdef) this belongs in validation; should never happen..
    def invalidTaskLostBehavior: ResidencyDefinition.TaskLostBehavior = {
      val allowedTaskLostBehaviorString =
        ResidencyDefinition.TaskLostBehavior.values().toSeq.map(_.getDescriptorForType.getName).mkString(", ")
      throw SerializationFailedException(
        s"'$residency' is not a valid taskLostBehavior. Allowed values: $allowedTaskLostBehaviorString")
    }
    Residency(
      relaunchEscalationTimeoutSeconds = residency.relaunchEscalationTimeoutSeconds.toLong,
      taskLostBehavior = residency.taskLostBehavior.fold(invalidTaskLostBehavior) {
        case TaskLostBehavior.RelaunchAfterTimeout => RELAUNCH_AFTER_TIMEOUT
        case TaskLostBehavior.WaitForever => WAIT_FOREVER
      }
    )
  }

  implicit val fetchUriReader = Reads[Artifact, FetchUri] { artifact =>
    FetchUri(
      uri = artifact.uri,
      extract = artifact.extract.getOrElse(FetchUri.defaultInstance.extract),
      executable = artifact.executable.getOrElse(FetchUri.defaultInstance.executable),
      cache = artifact.cache.getOrElse(FetchUri.defaultInstance.cache),
      outputFile = artifact.destPath.orElse(FetchUri.defaultInstance.outputFile)
    )
  }

  implicit val portDefinitionRamlReader = Reads[PortDefinition, state.PortDefinition] { portDef =>
    val protocol: String = portDef.protocol.map {
      case NetworkProtocol.Tcp => "tcp"
      case NetworkProtocol.Udp => "udp"
      case unexpected => throw SerializationFailedException(s"unexpected protocol $unexpected")
    }.getOrElse("tcp")

    state.PortDefinition(
      port = portDef.port.getOrElse(AppDefinition.RandomPortValue),
      protocol = protocol,
      name = portDef.name,
      labels = portDef.labels
    )
  }

  implicit val appHealthCheckRamlReader = Reads[AppHealthCheck, CoreHealthCheck] { check =>
    // TODO(jdef) move normalization somewhere else
    val normalized = check.protocol match {
      case Some(_) => check
      case None =>
        check.copy(protocol = check.command.map(_ => AppHealthCheckProtocol.Command)
          .orElse(Some(AppHealthCheckProtocol.Http)))
    }
    val result: CoreHealthCheck = normalized match {
      // TODO(jdef) raml lacks support for delay, maybe because marathon checks don't support it?
      case AppHealthCheck(Some(command), grace, None, interval, failures, None, None, None, Some(proto), timeout) =>
        if (proto != AppHealthCheckProtocol.Command) // TODO(jdef) move this to validation
          throw SerializationFailedException(s"illegal protocol $proto specified with command")
        MesosCommandHealthCheck(
          gracePeriod = grace.map(_.seconds).getOrElse(CoreHealthCheck.DefaultGracePeriod),
          interval = interval.map(_.seconds).getOrElse(CoreHealthCheck.DefaultInterval),
          timeout = timeout.map(_.seconds).getOrElse(CoreHealthCheck.DefaultTimeout),
          maxConsecutiveFailures = failures.getOrElse(CoreHealthCheck.DefaultMaxConsecutiveFailures),
          command = Command(command.items.value)
        )
      case AppHealthCheck(None, grace, ignore1xx, interval, failures, path, port, index, Some(proto), timeout) =>
        proto match {
          case AppHealthCheckProtocol.MesosHttp | AppHealthCheckProtocol.MesosHttps =>
            MesosHttpHealthCheck(
              gracePeriod = grace.map(_.seconds).getOrElse(CoreHealthCheck.DefaultGracePeriod),
              interval = interval.map(_.seconds).getOrElse(CoreHealthCheck.DefaultInterval),
              timeout = timeout.map(_.seconds).getOrElse(CoreHealthCheck.DefaultTimeout),
              maxConsecutiveFailures = failures.getOrElse(CoreHealthCheck.DefaultMaxConsecutiveFailures),
              portIndex = index.map(PortReference(_)),
              port = port,
              path = path,
              protocol =
                if (proto == AppHealthCheckProtocol.MesosHttp) Protos.HealthCheckDefinition.Protocol.MESOS_HTTP
                else Protos.HealthCheckDefinition.Protocol.MESOS_HTTPS
            )
          case AppHealthCheckProtocol.MesosTcp =>
            MesosTcpHealthCheck(
              gracePeriod = grace.map(_.seconds).getOrElse(CoreHealthCheck.DefaultGracePeriod),
              interval = interval.map(_.seconds).getOrElse(CoreHealthCheck.DefaultInterval),
              timeout = timeout.map(_.seconds).getOrElse(CoreHealthCheck.DefaultTimeout),
              maxConsecutiveFailures = failures.getOrElse(CoreHealthCheck.DefaultMaxConsecutiveFailures),
              portIndex = index.map(PortReference(_)),
              port = port
            )
          case AppHealthCheckProtocol.Http | AppHealthCheckProtocol.Https =>
            MarathonHttpHealthCheck(
              gracePeriod = grace.map(_.seconds).getOrElse(CoreHealthCheck.DefaultGracePeriod),
              interval = interval.map(_.seconds).getOrElse(CoreHealthCheck.DefaultInterval),
              timeout = timeout.map(_.seconds).getOrElse(CoreHealthCheck.DefaultTimeout),
              maxConsecutiveFailures = failures.getOrElse(CoreHealthCheck.DefaultMaxConsecutiveFailures),
              portIndex = index.map(PortReference(_)),
              port = port,
              path = path,
              protocol =
                if (proto == AppHealthCheckProtocol.MesosHttp) Protos.HealthCheckDefinition.Protocol.HTTP
                else Protos.HealthCheckDefinition.Protocol.HTTPS
            )
          case AppHealthCheckProtocol.Tcp =>
            MarathonTcpHealthCheck(
              gracePeriod = grace.map(_.seconds).getOrElse(CoreHealthCheck.DefaultGracePeriod),
              interval = interval.map(_.seconds).getOrElse(CoreHealthCheck.DefaultInterval),
              timeout = timeout.map(_.seconds).getOrElse(CoreHealthCheck.DefaultTimeout),
              maxConsecutiveFailures = failures.getOrElse(CoreHealthCheck.DefaultMaxConsecutiveFailures),
              portIndex = index.map(PortReference(_)),
              port = port
            )
          case _ =>
            throw SerializationFailedException(s"illegal protocol $proto for non-command health check")
        }
    }
    result
  }

  implicit val appVolumeRamlReader = Reads[AppVolume, state.Volume] { vol =>
    import scala.language.implicitConversions

    def failed[T](msg: String): T =
      throw new SerializationFailedException(msg)

    implicit def readRamlVolumeMode(m: Option[ReadMode]): mesos.Volume.Mode = m match {
      case Some(mode) => mode match {
        case ReadMode.Ro => mesos.Volume.Mode.RO
        case ReadMode.Rw => mesos.Volume.Mode.RW
      }
      case None => mesos.Volume.Mode.RW
    }

    vol match {
      case AppVolume(Some(ctPath), hostPath, None, Some(external), mode) =>
        val info = Some(ExternalVolumeInfo(
          size = external.size.map(_.toLong),
          name = external.name.getOrElse(failed("name is required for external volumes")),
          provider = external.provider.getOrElse(failed("provider is required for external volumes")),
          options = external.options
        ))
        state.Volume(containerPath = ctPath, hostPath = hostPath, mode = mode, persistent = None, external = info)
      case AppVolume(Some(ctPath), hostPath, Some(persistent), None, mode) =>
        val volType = persistent.`type` match {
          case Some(definedType) => definedType match {
            case PersistentVolumeType.Root => DiskType.Root
            case PersistentVolumeType.Mount => DiskType.Mount
            case PersistentVolumeType.Path => DiskType.Path
          }
          case None => DiskType.Root
        }
        val info = Some(PersistentVolumeInfo(
          size = persistent.size.toLong,
          maxSize = persistent.maxSize.map(_.toLong),
          `type` = volType,
          constraints = persistent.constraints.map { constraint =>
            (constraint.headOption, constraint.lift(1), constraint.lift(2)) match {
              case (Some("path"), Some("LIKE"), Some(value)) =>
                Protos.Constraint.newBuilder()
                  .setField("path")
                  .setOperator(Protos.Constraint.Operator.LIKE)
                  .setValue(value)
                  .build()
              case _ =>
                throw SerializationFailedException(s"illegal volume constraint ${constraint.mkString(",")}")
            }
          }(collection.breakOut)
        ))
        state.Volume(containerPath = ctPath, hostPath = hostPath, mode = mode, persistent = info, external = None)
      case AppVolume(Some(ctPath), hostPath, None, None, mode) =>
        state.Volume(containerPath = ctPath, hostPath = hostPath, mode = mode, persistent = None, external = None)
      case v => failed(s"illegal volume specification $v")
    }
  }

  implicit val portMappingRamlReader = Reads[ContainerPortMapping, state.Container.PortMapping] {
    case ContainerPortMapping(containerPort, hostPort, labels, name, protocol, servicePort) =>
      import state.Container.PortMapping._
      val decodedProto = protocol match {
        case Some(definedProto) => definedProto match {
          case DockerPortProtocol.Tcp => TCP
          case DockerPortProtocol.Udp => UDP
          case DockerPortProtocol.UdpTcp => UDP_TCP
        }
        case None => defaultInstance.protocol
      }
      state.Container.PortMapping(
        containerPort = containerPort,
        hostPort = hostPort.orElse(defaultInstance.hostPort),
        servicePort = servicePort.getOrElse(defaultInstance.servicePort),
        protocol = decodedProto,
        name = name,
        labels = labels
      )
  }

  implicit val appContainerRamlReader = Reads[Container, state.Container] { (container: Container) =>
    val volumes = container.volumes.map(Raml.fromRaml(_))
    val portMappings = container.portMappings.map(Raml.fromRaml(_))

    val result: state.Container = (container.`type`, container.docker, container.appc) match {
      case (EngineType.Docker, Some(docker), None) =>
        state.Container.Docker(
          volumes = volumes,
          image = docker.image,
          portMappings = portMappings, // assumed already normalized, see Formats
          privileged = docker.privileged.getOrElse(false),
          parameters = docker.parameters.map(p => Parameter(p.key, p.value)),
          forcePullImage = docker.forcePullImage.getOrElse(false)
        )
      case (EngineType.Mesos, Some(docker), None) =>
        state.Container.MesosDocker(
          volumes = volumes,
          image = docker.image,
          portMappings = portMappings, // assumed already normalized, see Formats
          credential = docker.credential.map(c => state.Container.Credential(principal = c.principal, secret = c.secret)),
          forcePullImage = docker.forcePullImage.getOrElse(false)
        )
      case (EngineType.Mesos, None, Some(appc)) =>
        state.Container.MesosAppC(
          volumes = volumes,
          image = appc.image,
          portMappings = portMappings,
          id = appc.id,
          labels = appc.labels,
          forcePullImage = appc.forcePullImage.getOrElse(false)
        )
      case (EngineType.Mesos, None, None) =>
        state.Container.Mesos(
          volumes = volumes,
          portMappings = portMappings
        )
      case ct => throw SerializationFailedException(s"illegal container specification $ct")
    }
    result
  }

  implicit val appReadinessRamlReader = Reads[ReadinessCheck, CoreReadinessCheck] { check =>
    val protocol = check.protocol.map {
      case HttpScheme.Http => CoreReadinessCheck.Protocol.HTTP
      case HttpScheme.Https => CoreReadinessCheck.Protocol.HTTPS
    }
    val result: CoreReadinessCheck = CoreReadinessCheck(
      name = check.name.getOrElse(CoreReadinessCheck.DefaultName),
      protocol = protocol.getOrElse(CoreReadinessCheck.DefaultProtocol),
      path = check.path.getOrElse(CoreReadinessCheck.DefaultPath),
      portName = check.portName.getOrElse(CoreReadinessCheck.DefaultPortName),
      interval = check.intervalSeconds.seconds,
      timeout = check.timeoutSeconds.seconds,
      httpStatusCodesForReady = check.httpStatusCodesForReady.toSet,
      preserveLastResponse = check.preserveLastResponse.getOrElse(CoreReadinessCheck.DefaultPreserveLastResponse)
    )
    result
  }

  implicit val upgradeStrategyRamlReader = Reads[UpgradeStrategy, state.UpgradeStrategy] { us =>
    import AppDefinition.DefaultUpgradeStrategy
    state.UpgradeStrategy(
      maximumOverCapacity = us.maximumOverCapacity.getOrElse(DefaultUpgradeStrategy.maximumOverCapacity),
      minimumHealthCapacity = us.minimumHealthCapacity.getOrElse(DefaultUpgradeStrategy.minimumHealthCapacity)
    )
  }

  implicit val appRamlReader: Reads[App, AppDefinition] = Reads[App, AppDefinition] { app =>
    // TODO not all validation has been applied to the raml; most app validation still just validates the model.
    // there's also some code here that would probably be better off in a raml.App `normalization` func.

    val selectedStrategy = ResidencyAndUpgradeStrategy(
      app.residency.map(Raml.fromRaml(_)),
      app.upgradeStrategy.map(Raml.fromRaml(_)),
      app.container.exists(_.volumes.exists(_.persistent.nonEmpty)),
      app.container.exists(_.volumes.exists(_.external.nonEmpty))
    )

    val backoffStrategy = BackoffStrategy(
      backoff = app.backoffSeconds.map(_.seconds).getOrElse(AppDefinition.DefaultBackoff),
      maxLaunchDelay = app.maxLaunchDelaySeconds.map(_.seconds).getOrElse(AppDefinition.DefaultMaxLaunchDelay),
      factor = app.backoffFactor.getOrElse(AppDefinition.DefaultBackoffFactor)
    )

    val versionInfo = state.VersionInfo.OnlyVersion(app.version.map(Timestamp(_)).getOrElse(Timestamp.now()))

    val result: AppDefinition = AppDefinition(
      id = PathId(app.id),
      cmd = app.cmd,
      args = app.args,
      user = app.user,
      env = Raml.fromRaml(app.env),
      instances = app.instances.getOrElse(AppDefinition.DefaultInstances),
      resources = resources(app.cpus, app.mem, app.disk, app.gpus),
      executor = app.executor.getOrElse(AppDefinition.DefaultExecutor),
      constraints = app.constraints.map(Raml.fromRaml(_))(collection.breakOut),
      fetch = app.fetch.map(Raml.fromRaml(_)),
      storeUrls = app.storeUrls,
      portDefinitions = app.portDefinitions.map(Raml.fromRaml(_)),
      requirePorts = app.requirePorts.getOrElse(AppDefinition.DefaultRequirePorts),
      backoffStrategy = backoffStrategy,
      container = app.container.map(Raml.fromRaml(_)),
      healthChecks = app.healthChecks.map(Raml.fromRaml(_)).toSet,
      readinessChecks = app.readinessChecks.map(Raml.fromRaml(_)),
      taskKillGracePeriod = app.taskKillGracePeriodSeconds.map(_.second).orElse(AppDefinition.DefaultTaskKillGracePeriod),
      dependencies = app.dependencies.map(PathId(_))(collection.breakOut),
      upgradeStrategy = selectedStrategy.upgradeStrategy,
      labels = app.labels,
      acceptedResourceRoles = app.acceptedResourceRoles,
      networks = app.networks.map(Raml.fromRaml(_)),
      versionInfo = versionInfo,
      residency = selectedStrategy.residency,
      secrets = Raml.fromRaml(app.secrets)
    )
    result
  }

  implicit val appUpdateRamlReader: Reads[(AppUpdate, AppDefinition), AppDefinition] = Reads { src =>
    val (update: AppUpdate, app: AppDefinition) = src
    app.copy(
      // id stays the same
      cmd = update.cmd.orElse(app.cmd),
      args = update.args.getOrElse(app.args),
      user = update.user.orElse(app.user),
      env = update.env.fold(app.env)(Raml.fromRaml(_)),
      instances = update.instances.getOrElse(app.instances),
      resources = Resources(
        cpus = update.cpus.getOrElse(app.resources.cpus),
        mem = update.mem.getOrElse(app.resources.mem),
        disk = update.disk.getOrElse(app.resources.disk),
        gpus = update.gpus.getOrElse(app.resources.gpus)
      ),
      executor = update.executor.getOrElse(app.executor),
      constraints = update.constraints.fold(app.constraints)(c => c.map(Raml.fromRaml(_))(collection.breakOut)),
      fetch = update.fetch.fold(app.fetch)(f => f.map(Raml.fromRaml(_))),
      storeUrls = update.storeUrls.getOrElse(app.storeUrls),
      portDefinitions = update.portDefinitions.fold(app.portDefinitions)(p => p.map(Raml.fromRaml(_))),
      requirePorts = update.requirePorts.getOrElse(app.requirePorts),
      backoffStrategy = BackoffStrategy(
        backoff = update.backoffSeconds.fold(app.backoffStrategy.backoff)(_.seconds),
        factor = update.backoffFactor.getOrElse(app.backoffStrategy.factor),
        maxLaunchDelay = update.maxLaunchDelaySeconds.fold(app.backoffStrategy.maxLaunchDelay)(_.seconds)
      ),
      container = update.container.map(Raml.fromRaml(_)).orElse(app.container),
      healthChecks = update.healthChecks.fold(app.healthChecks)(h => h.map(Raml.fromRaml(_)).toSet),
      readinessChecks = update.readinessChecks.fold(app.readinessChecks)(r => r.map(Raml.fromRaml(_))),
      dependencies = update.dependencies.fold(app.dependencies)(deps => deps.map(PathId(_).canonicalPath(app.id))(collection.breakOut)),
      upgradeStrategy = update.upgradeStrategy.fold(app.upgradeStrategy)(Raml.fromRaml(_)),
      labels = update.labels.getOrElse(app.labels),
      acceptedResourceRoles = update.acceptedResourceRoles.getOrElse(app.acceptedResourceRoles),
      networks = update.networks.fold(app.networks)(nets => nets.map(Raml.fromRaml(_))),
      // versionInfo doesn't change - it's never overridden by an AppUpdate
      // Setting the version in AppUpdate means that the user wants to revert to that version. In that
      // case, we do not update the current AppDefinition but revert completely to the specified version.
      // For all other updates, the GroupVersioningUtil will determine a new version if the AppDefinition
      // has really changed.
      residency = update.residency.map(Raml.fromRaml(_)).orElse(app.residency),
      secrets = update.secrets.fold(app.secrets)(Raml.fromRaml(_)),
      taskKillGracePeriod = update.taskKillGracePeriodSeconds.map(_.seconds).orElse(app.taskKillGracePeriod)
    )
  }
}

object AppConversion extends AppConversion {

  case class ResidencyAndUpgradeStrategy(residency: Option[Residency], upgradeStrategy: state.UpgradeStrategy)

  object ResidencyAndUpgradeStrategy {
    def apply(
      residency: Option[Residency],
      upgradeStrategy: Option[state.UpgradeStrategy],
      hasPersistentVolumes: Boolean,
      hasExternalVolumes: Boolean): ResidencyAndUpgradeStrategy = {

      import state.UpgradeStrategy.{ empty, forResidentTasks }

      val residencyOrDefault: Option[ Residency ] =
        residency.orElse(if (hasPersistentVolumes) Some(Residency.defaultResidency) else None)

      val selectedUpgradeStrategy = upgradeStrategy.getOrElse {
        if (residencyOrDefault.isDefined || hasExternalVolumes) forResidentTasks else empty
      }

      ResidencyAndUpgradeStrategy(residencyOrDefault, selectedUpgradeStrategy)
    }
  }
}
