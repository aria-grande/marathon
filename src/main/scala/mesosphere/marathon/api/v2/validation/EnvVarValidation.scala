package mesosphere.marathon
package api.v2.validation

import com.wix.accord._
import com.wix.accord.dsl._
import mesosphere.marathon.api.v2.Validation
import mesosphere.marathon.raml.{ EnvVarSecretRef, EnvVarValueOrSecret, SecretDef }

trait EnvVarValidation {
  import Validation._
  import SecretValidation._

  val EnvVarNamePattern = """^[A-Z_][A-Z0-9_]*$""".r

  val validEnvVarName: Validator[String] = validator[String] { name =>
    name should matchRegexWithFailureMessage(
      EnvVarNamePattern,
      "must contain only alphanumeric chars or underscore, and must not begin with a number")
    name.length should be > 0
    name.length should be < 255
  }

  def envValidator(secrets: Map[String, SecretDef], enabledFeatures: Set[String]) = validator[Map[String, EnvVarValueOrSecret]] { env =>
    env.keys is every(validEnvVarName)

    // if the secrets feature is not enabled then don't allow EnvVarSecretRef's in the environment
    env is isTrue("use of secret-references in the environment requires the secrets feature to be enabled") { env =>
      if (!enabledFeatures.contains(Features.SECRETS))
        env.values.count {
          case _: EnvVarSecretRef => true
          case _ => false
        } == 0
      else true
    }

    env is every(secretRefValidator(secrets))
  }
}

object EnvVarValidation extends EnvVarValidation
