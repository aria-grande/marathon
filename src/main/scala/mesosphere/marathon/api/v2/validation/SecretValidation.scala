package mesosphere.marathon
package api.v2.validation

import com.wix.accord.dsl._
import mesosphere.marathon.api.v2.Validation
import mesosphere.marathon.raml.{ EnvVarSecretRef, EnvVarValueOrSecret, SecretDef }

trait SecretValidation {
  import Validation._

  def secretRefValidator(secrets: Map[String, SecretDef]) = validator[(String, EnvVarValueOrSecret)] { entry =>
    entry._2 as s"${entry._1}" is isTrue("references an undefined secret"){
      case ref: EnvVarSecretRef => secrets.contains(ref.secret)
      case _ => true
    }
  }

  val secretValidator = validator[Map[String, SecretDef]] { s =>
    s.keys is every(notEmpty)
    s.values.map(_.source) as "source" is every(notEmpty)
  }
}

object SecretValidation extends SecretValidation
