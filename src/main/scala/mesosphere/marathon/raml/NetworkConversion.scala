package mesosphere.marathon.raml

import mesosphere.marathon.core.pod

trait NetworkConversion {
  implicit val networkRamlReader: Reads[Network, pod.Network] =
    Reads { raml =>
      raml.mode match {
        case NetworkMode.Host => pod.HostNetwork
        case NetworkMode.ContainerBridge => pod.BridgeNetwork(raml.labels)
        case NetworkMode.Container => pod.ContainerNetwork(
          // TODO(PODS): shouldn't this be caught by validation?
          raml.name.getOrElse(throw new IllegalArgumentException("container network must specify a name")),
          raml.labels
        )
      }
    }

  implicit val networkRamlWriter: Writes[pod.Network, Network] = Writes {
    case cnet: pod.ContainerNetwork =>
      Network(
        name = Some(cnet.name),
        mode = NetworkMode.Container,
        labels = cnet.labels
      )
    case br: pod.BridgeNetwork =>
      Network(
        mode = NetworkMode.ContainerBridge,
        labels = br.labels
      )
    case pod.HostNetwork => Network(mode = NetworkMode.Host)
  }
}

object NetworkConversion extends NetworkConversion
