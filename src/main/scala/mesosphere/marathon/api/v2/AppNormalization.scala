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

  // TODO(jdef) incorporate default network name
  def apply(update: AppUpdate): AppUpdate = {
    val fetch = normalizeFetch(update.uris, update.fetch)
    val networks = NetworkTranslation.toNetworks(NetworkTranslation(
      update.ipAddress,
      update.container.flatMap(_.docker.flatMap(_.network)),
      update.networks
    ))
    val container = normalizePortMappings(networks, update.container)
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

  // TODO(jdef) incorporate default network name
  def apply(before: App): App = {
    val fetch: Seq[Artifact] = normalizeFetch(Option(before.uris), Option(before.fetch)).getOrElse(Nil)
    val networks: Seq[ Network ] = NetworkTranslation.toNetworks(NetworkTranslation(
      before.ipAddress,
      before.container.flatMap(_.docker.flatMap(_.network)),
      Some(before.networks)
    )).getOrElse(Nil)

    // Normally, our default is one port. If an ipAddress is defined that would lead to an error
    // if left unchanged.
    def portDefinitions: Seq[PortDefinition] =
      if (networks.exists(_.mode != NetworkMode.Host))
        Nil
      else if (before.portDefinitions.nonEmpty)
        before.portDefinitions
      else if (before.ports.nonEmpty)
        before.ports.map(p => PortDefinition(port = Some(p)))
      else
        Seq(PortDefinition(port = Some(0)))

    val container = normalizePortMappings(Some(networks), before.container)

    before.copy(
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
      healthChecks = normalizeHealthChecks(before.healthChecks)
    )
  }
}

object AppNormalization extends AppNormalization {

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
            throw SerializationFailedException(s"unsupported docker network type ${unsupported}")
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
        ???
        // user didn't ask for IP-per-CT, but specified a network type anyway
        import DockerNetwork._
        networkType match {
          case Host => Some(Seq(Network(mode = NetworkMode.Host)))
          case User => Some(Seq(Network(mode = NetworkMode.Container)))
          case Bridge => Some(Seq(Network(mode = NetworkMode.ContainerBridge)))
          case unsupported =>
            throw SerializationFailedException(s"unsupported docker network type ${unsupported}")
        }
      case NetworkTranslation(None, None, networks) =>
        // no deprecated APIs used! awesome, so use the canonical networks field
        networks
    }
  }
}
