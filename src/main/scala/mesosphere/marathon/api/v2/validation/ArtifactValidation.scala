package mesosphere.marathon
package api.v2.validation

import java.net.URI
import scala.util.Try

import com.wix.accord._
import com.wix.accord.dsl._
import mesosphere.marathon.raml.Artifact

trait ArtifactValidation {
  val uriValidator: Validator[String] = new Validator[String] {
    def apply(uri: String) =
      Try(new URI(uri)).toOption.map(_ => Success).getOrElse(
        Failure(Set(RuleViolation(uri, "URI has invalid syntax.", None)))
      )
  }
  implicit val artifactUriIsValid: Validator[Artifact] = validator[Artifact] { artifact =>
    artifact.uri is valid(uriValidator)
  }
}

object ArtifactValidation extends ArtifactValidation
