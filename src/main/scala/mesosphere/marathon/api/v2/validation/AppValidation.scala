package mesosphere.marathon
package api.v2.validation

import com.wix.accord._
import com.wix.accord.dsl._
import mesosphere.marathon.api.v2.Validation._
import mesosphere.marathon.raml._
import mesosphere.marathon.state.{ AppDefinition, PathId, PortAssignment, ResourceRole }

trait AppValidation {
  import AppValidation._
  import ArtifactValidation._
  import EnvVarValidation._
  import SecretValidation._
  import SchedulingValidator._

  implicit val appResidencyValidator: Validator[AppResidency] = validator[AppResidency] { residency =>
    residency.relaunchEscalationTimeoutSeconds >= 0
  }

  implicit val portDefinitionValidator = validator[PortDefinition] { portDefinition =>
    portDefinition.port should optional(be >= 0)
    portDefinition.name is optional(matchRegexFully(PortAssignment.PortNamePattern))
  }

  val portDefinitionsValidator: Validator[Seq[PortDefinition]] = validator[Seq[PortDefinition]] {
    portDefinitions =>
      portDefinitions is every(valid)
      portDefinitions is elementsAreUniqueByOptional(_.name, "Port names must be unique.")
      portDefinitions is elementsAreUniqueBy(_.port, "Ports must be unique.",
        filter = { port: Option[Int] => port.fold(false)(_ != AppDefinition.RandomPortValue) })
  }

  val dockerDockerContainerValidator: Validator[Container] = ???
  val mesosDockerContainerValidator: Validator[Container] = ???
  val mesosAppcContainerValidator: Validator[Container] = ???
  val mesosContainerValidator: Validator[Container] = ???

  def validContainer(/*enabledFeatures: Set[String]*/): Validator[Container] = {
    val validGeneralContainer: Validator[ Container ] = ??? // TODO(jdef) validate volumes, portMappings

    val validSpecializedContainer = new Validator[Container] {
      override def apply(container: Container): Result = {
        (container.docker, container.appc, container.`type`) match {
          case (Some(docker), None, EngineType.Docker) => validate(container)(dockerDockerContainerValidator)
          case (Some(docker), None, EngineType.Mesos) => validate(container)(mesosDockerContainerValidator)
          case (None, Some(appc), EngineType.Mesos) => validate(container)(mesosAppcContainerValidator)
          case (None, None, EngineType.Mesos) => validate(container)(mesosContainerValidator)
          case _ => Failure(Set(RuleViolation(container, "illegal combination of containerizer and image type", None)))
        }
      }
    }
    validGeneralContainer and validSpecializedContainer
  }

  def appUpdateValidator(enabledFeatures: Set[String]): Validator[AppUpdate] = validator[AppUpdate] { update =>
    update.id.map(PathId(_)) as "id" is optional(valid)
    update.executor.each should matchRegex(executorPattern)
    update.mem should optional(be >= 0.0)
    update.cpus should optional(be >= 0.0)
    update.instances should optional(be >= 0)
    update.disk should optional(be >= 0.0)
    update.gpus should optional(be >= 0)
    update.dependencies.map(_.map(PathId(_))) as "dependencies" is optional(every(valid))
    update.env is optional(envValidator(update.secrets.getOrElse(Map.empty), enabledFeatures))
    update.secrets is optional(isTrue("secrets feature is required for using secrets with an application"){
      (secrets: Map[String,SecretDef]) =>
        if (secrets.nonEmpty)
          validateOrThrow(secrets)(secretValidator and featureEnabled(enabledFeatures, Features.SECRETS))
        true
    })
    update.storeUrls is optional(every(urlCanBeResolvedValidator))
    update.fetch is optional(every(valid))
    update.upgradeStrategy is optional(valid)
    update.residency is optional(valid)
    update.portDefinitions is optional(portDefinitionsValidator)
    // TODO(jdef) validate ports?
    // TODO(jdef) validate uris?
    // TODO(jdef) validate ipAddress?
    update.container is optional(validContainer(/*enabledFeatures*/))
    // TODO(jdef) fix the RAML type (uniqueItems: true) and drop the map(_.toSet) here
    update.acceptedResourceRoles.map(_.toSet) as "acceptedResourceRoles" is optional(ResourceRole.validAcceptedResourceRoles(update.residency.isDefined))
  } and isTrue("must not be root") { (update: AppUpdate) =>
    !update.id.fold(false)(PathId(_).isRoot)
  } and isTrue("must not be an empty string") { (update: AppUpdate) =>
    update.cmd.forall { s => s.length() > 1 }
  } and isTrue("ports must be unique") { (update: AppUpdate) =>
    val withoutRandom = update.ports.fold(Seq.empty[Int])(_.filterNot(_ == AppDefinition.RandomPortValue))
    withoutRandom.distinct.size == withoutRandom.size
  } and isTrue("cannot specify both an IP address and port") { (update: AppUpdate) =>
    val appWithoutPorts = update.ports.fold(true)(_.isEmpty) && update.portDefinitions.fold(true)(_.isEmpty)
    appWithoutPorts || update.ipAddress.isEmpty
  } and isTrue("cannot specify both ports and port definitions") { (update: AppUpdate) =>
    val portDefinitionsIsEquivalentToPorts = update.portDefinitions.map(_.map(_.port)) == update.ports.map(_.map(Some(_)))
    portDefinitionsIsEquivalentToPorts || update.ports.isEmpty || update.portDefinitions.isEmpty
  } and isTrue("must not specify both networks and ipAddress") { (update: AppUpdate) =>
    !(update.ipAddress.nonEmpty && update.networks.fold(false)(_.nonEmpty))
  } and isTrue("must not specify both container.docker.network and networks") { (update: AppUpdate) =>
    !(update.container.exists(_.docker.exists(_.network.nonEmpty)) && update.networks.nonEmpty)
  }

  // TODO(jdef) check for portMappings and portDefinitions both assigned?

  implicit val appRamlValidator: Validator[App] = validator[App] { app =>
    app.executor.each should matchRegex(executorPattern)
  } and isTrue("must not be root") { (app: App) =>
    !PathId(app.id).isRoot
  } and isTrue("must not be an empty string") { (app: App) =>
    app.cmd.forall { s => s.length() > 1 }
  } and isTrue("ports must be unique") { (app: App) =>
    val withoutRandom = app.ports.filterNot(_ == AppDefinition.RandomPortValue)
    withoutRandom.distinct.size == withoutRandom.size
  } and isTrue("cannot specify both an IP address and port") { (app: App) =>
    val appWithoutPorts = app.ports.isEmpty && app.portDefinitions.isEmpty
    appWithoutPorts || app.ipAddress.isEmpty
  } and isTrue("cannot specify both ports and port definitions") { (app: App) =>
    val portDefinitionsIsEquivalentToPorts = app.portDefinitions.map(_.port) == app.ports.map(Some(_))
    portDefinitionsIsEquivalentToPorts || app.ports.isEmpty || app.portDefinitions.isEmpty
  } and isTrue("must not specify both networks and ipAddress") { (app: App) =>
    !(app.ipAddress.nonEmpty && app.networks.nonEmpty)
  } and isTrue("must not specify both container.docker.network and networks") { (app: App) =>
    !(app.container.exists(_.docker.exists(_.network.nonEmpty)) && app.networks.nonEmpty)
  }

  def validNestedApp(base: PathId, enabledFeatures: Set[String]): Validator[App] = ??? // TODO(jdef) refactor from AppDefinition impl
}

object AppValidation extends AppValidation {

  val executorPattern = "^(//cmd)|(/?[^/]+(/[^/]+)*)|$".r
}
