package mesosphere.marathon
package api.v2

import mesosphere.marathon.raml._
import mesosphere.marathon.state.FetchUri
import mesosphere.mesos.TaskBuilder

trait AppNormalization {

  import AppNormalization._

  /**
    * Ensure backwards compatibility by adding portIndex to health checks when necessary.
    *
    * In the past, healthCheck.portIndex was required and had a default value 0. When we introduced healthCheck.port, we
    * made it optional (also with ip-per-container in mind) and we have to re-add it in cases where it makes sense.
    */
  def normalizeHealthChecks(healthChecks: Seq[AppHealthCheck]): Seq[AppHealthCheck] = {
    def withPort(check: AppHealthCheck): AppHealthCheck = {
      def needsDefaultPortIndex = check.port.isEmpty && check.portIndex.isEmpty
      if (needsDefaultPortIndex) check.copy(portIndex = Some(0)) else check
    }

    import AppHealthCheckProtocol._
    healthChecks.map {
      case check: AppHealthCheck if check.protocol.exists{ p =>
        p == Tcp || p == Http || p == Https || p == MesosTcp || p == MesosHttp || p == MesosHttps
      } || (check.command.isEmpty && check.protocol.isEmpty) => withPort(check)
      case check => check
    }
  }

  def normalizeFetch(uris: Option[Seq[String]], fetch: Option[Seq[Artifact]]): Option[Seq[Artifact]] =
    if (uris.fold(false)(_.nonEmpty) && fetch.fold(false)(_.nonEmpty))
      throw SerializationFailedException("cannot specify both uris and fetch fields")
    else
       uris.fold(fetch){
         uris => Some(uris.map(uri => Artifact(uri = uri, extract = Some(FetchUri.isExtract(uri)))))
       }

  /**
    * currently invoked prior to validation, so that we only validate portMappings once
    */
  def normalizeDocker(container: Container): Container = {
    def translatePortMappings(dockerPortMappings: Seq[ContainerPortMapping]): Seq[ContainerPortMapping] =
      (container.portMappings.isEmpty, dockerPortMappings.isEmpty) match {
        case (false, false) =>
          throw SerializationFailedException("cannot specify both portMappings and docker.portMappings")
        case (false, true) =>
          container.portMappings
        case (true, false) =>
          dockerPortMappings
        case _ =>
          Nil
      }

    container.docker.map(_.portMappings) match {
      case Some(portMappings) => container.copy(
        portMappings = translatePortMappings(portMappings),
        docker = container.docker.map(_.copy(portMappings = Nil))
      )
      case None => container
    }

    // note, we leave container.docker.network alone because we'll need that for app normalization
  }

  def normalizePortMappings(networks: Option[Seq[Network]], container: Option[Container]): Option[Container] = {
    // assuming that we're already validated and everything ELSE network-related has been normalized, we can now
    // deal with translating unspecified port-mapping host-port's when in bridge mode
    val isBridgedNetwork = networks.fold(false)(_.exists(_.mode == NetworkMode.ContainerBridge))
    container.map { ct =>
      ct.copy(
        docker = ct.docker.map { d =>
          // this is deprecated, clear it so that it's deterministic later on...
          d.copy(network = None)
        },
        portMappings =
          if (!isBridgedNetwork) ct.portMappings
          else ct.portMappings.map {
            // backwards compat: when in BRIDGE mode, missing host ports default to zero
            case ContainerPortMapping(x, None, y, z, w, a) =>
              ContainerPortMapping(x, Some(state.Container.PortMapping.HostPortDefault), y, z, w, a)
            case m => m
          }
      )
    }
  }

  /**
    * only deprecated fields and their interaction with canonical fields have been validated so far,
    * so we limit normalization here to translating from the deprecated API to the canonical one.
    *
    * @return an API object in canonical form (read: doesn't use deprecated APIs)
    */
  def forDeprecatedFields(update: AppUpdate): AppUpdate = {
    val fetch = normalizeFetch(update.uris, update.fetch)
    val container = update.container.map(normalizeDocker)
    val networks = NetworkTranslation.toNetworks(NetworkTranslation(
      update.ipAddress,
      container.flatMap(_.docker.flatMap(_.network)),
      update.networks
    ))

    val portDefinitions = update.portDefinitions.orElse(
      update.ports.map(_.map(port => PortDefinition(port = Some(port)))))

    update.copy(
      // normalize fetch
      fetch = fetch,
      uris = None,
      // normalize networks
      networks = networks,
      ipAddress = None,
      container = container,
      // ports
      portDefinitions = portDefinitions,
      ports = None,
      // health checks
      healthChecks = update.healthChecks.map(normalizeHealthChecks)
    )
  }

  def apply(update: AppUpdate, config: Config): AppUpdate = {
    val networks = config.defaultNetworkName.map { _ =>
      update.networks.map(_.map {
        case n: Network if n.name.isEmpty && n.mode == NetworkMode.Container => n.copy(name = config.defaultNetworkName)
        case n => n
      })
    }.getOrElse(update.networks)

    val container = normalizePortMappings(update.networks, update.container)
    update.copy(
      container = container,
      networks = networks
    )
  }

  /**
    * only deprecated fields and their interaction with canonical fields have been validated so far,
    * so we limit normalization here to translating from the deprecated API to the canonical one.
    *
    * @return an API object in canonical form (read: doesn't use deprecated APIs)
    */
  def forDeprecatedFields(app: App): App = {
    val fetch: Seq[Artifact] = normalizeFetch(Option(app.uris), Option(app.fetch)).getOrElse(Nil)
    val container = app.container.map(normalizeDocker)
    val networks: Seq[ Network ] = NetworkTranslation.toNetworks(NetworkTranslation(
      app.ipAddress,
      container.flatMap(_.docker.flatMap(_.network)),
      Some(app.networks)
    )).getOrElse(Nil)

    // Normally, our default is one port. If an non-host networks are defined that would lead to an error
    // if left unchanged.
    def portDefinitions: Seq[PortDefinition] =
      if (networks.exists(_.mode != NetworkMode.Host))
        Nil
      else if (app.portDefinitions.nonEmpty)
        app.portDefinitions
      else if (app.ports.nonEmpty)
        app.ports.map(p => PortDefinition(port = Some(p)))
      else
        Seq(PortDefinition(port = Some(0)))

    app.copy(
      // normalize fetch
      fetch = fetch,
      uris = Nil,
      // normalize networks
      networks = networks,
      ipAddress = None,
      container = container,
      // normalize ports
      portDefinitions = portDefinitions,
      ports = Nil,
      // health checks
      healthChecks = normalizeHealthChecks(app.healthChecks)
    )
  }

  def apply(app: App, config: Config): App = {
    val networks = config.defaultNetworkName.map { _ =>
      app.networks.map {
        case n: Network if n.name.isEmpty && n.mode == NetworkMode.Container => n.copy(name = config.defaultNetworkName)
        case n => n
      }
    }.getOrElse(app.networks)

    val container = normalizePortMappings(Some(app.networks), app.container)

    app.copy(
      container = container,
      networks = networks
    )
  }
}

object AppNormalization extends AppNormalization {

  case class Config(defaultNetworkName: Option[String])

  /**
    * attempt to translate an older app API (that uses ipAddress and container.docker.network) to the new API
    * (that uses app.networks, and container.portMappings)
    */
  case class NetworkTranslation(
    ipAddress: Option[IpAddress],
    networkType: Option[DockerNetwork],
    networks: Option[Seq[Network]])

  object NetworkTranslation {
    def toNetworks(nt: NetworkTranslation): Option[Seq[Network]] = nt match {
      case NetworkTranslation(Some(ipAddress), Some(networkType), _) =>
        // wants ip/ct with a specific network mode
        import DockerNetwork._
        networkType match {
          case Host =>
            Some(Seq(Network(mode = NetworkMode.Host))) // strange way to ask for this, but we'll accommodate
          case User =>
            Some(Seq(Network(mode = NetworkMode.Container, name = ipAddress.networkName, labels = ipAddress.labels)))
          case Bridge =>
            Some(Seq(Network(mode = NetworkMode.ContainerBridge, labels = ipAddress.labels)))
          case unsupported =>
            throw SerializationFailedException(s"unsupported docker network type $unsupported")
        }
      case NetworkTranslation(Some(ipAddress), None, _) =>
        // wants ip/ct with some network mode.
        // if the user gave us a name try to figure out what they want.
        ipAddress.networkName match {
          case Some(name) if name == TaskBuilder.MesosBridgeName => // users shouldn't do this, but we're tolerant
            Some(Seq(Network(mode = NetworkMode.ContainerBridge)))
          case name =>
            Some(Seq(Network(mode = NetworkMode.Container, name = name)))
        }
      case NetworkTranslation(None, Some(networkType), _) =>
        // user didn't ask for IP-per-CT, but specified a network type anyway
        import DockerNetwork._
        networkType match {
          case Host => Some(Seq(Network(mode = NetworkMode.Host)))
          case User => Some(Seq(Network(mode = NetworkMode.Container)))
          case Bridge => Some(Seq(Network(mode = NetworkMode.ContainerBridge)))
          case unsupported =>
            throw SerializationFailedException(s"unsupported docker network type $unsupported")
        }
      case NetworkTranslation(None, None, networks) =>
        // no deprecated APIs used! awesome, so use the canonical networks field
        networks
    }
  }
}
