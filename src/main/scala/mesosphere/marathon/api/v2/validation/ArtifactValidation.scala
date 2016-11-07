package mesosphere.marathon
package api.v2.validation

import java.net.{ URI, URISyntaxException }

import com.wix.accord._
import mesosphere.marathon.raml.Artifact

trait ArtifactValidation {
  implicit val artifactUriIsValid: Validator[Artifact] = {
    new Validator[Artifact] {
      def apply(uri: Artifact) = {
        try {
          new URI(uri.uri)
          Success
        } catch {
          case _: URISyntaxException => Failure(Set(RuleViolation(uri.uri, "URI has invalid syntax.", None)))
        }
      }
    }
  }
}

object ArtifactValidation extends ArtifactValidation
