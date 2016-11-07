package mesosphere.marathon
package core.pod

import mesosphere.marathon.stream._
import mesosphere.marathon.Protos.NetworkDefinition
import org.apache.mesos.{ Protos => Mesos }

/**
  * Network declared by a [[PodDefinition]].
  */
sealed trait Network extends Product with Serializable

case object HostNetwork extends Network

case class ContainerNetwork(name: String, labels: Map[String, String] = Network.DefaultLabels) extends Network

/** specialized container network that uses a default bridge (containerizer dependent) */
case class BridgeNetwork(labels: Map[String, String] = Network.DefaultLabels) extends Network

object Network {

  val DefaultLabels: Map[String, String] = Map.empty

  def fromProto(net: NetworkDefinition): Option[Network] = {
    import NetworkDefinition.Mode._

    def labelsFromProto: Map[String, String] = net.getLabelsList.collect {
      case label if label.hasKey && label.hasValue => label.getKey -> label.getValue
    }(collection.breakOut)

    net.getMode() match {
      case UNKNOWN =>
        None
      case HOST =>
        Some(HostNetwork)
      case CONTAINER =>
        val name =
          if (net.hasName) net.getName
          else throw new IllegalStateException("missing container name in NetworkDefinition")
        Some(ContainerNetwork(name, labelsFromProto))
      case BRIDGE =>
        Some(BridgeNetwork(labelsFromProto))
    }
  }

  def toProto(net: Network): NetworkDefinition = {
    val builder = NetworkDefinition.newBuilder()
    net match {
      case HostNetwork =>
        builder.setMode(NetworkDefinition.Mode.HOST)
      case br: BridgeNetwork =>
        builder
          .setMode(NetworkDefinition.Mode.BRIDGE)
          .addAllLabels(br.labels.map{ case (k, v) => Mesos.Label.newBuilder().setKey(k).setValue(v).build() })
      case ct: ContainerNetwork =>
        builder
          .setMode(NetworkDefinition.Mode.CONTAINER)
          .setName(ct.name)
          .addAllLabels(ct.labels.map{ case (k, v) => Mesos.Label.newBuilder().setKey(k).setValue(v).build() })
    }
    builder.build()
  }
}
