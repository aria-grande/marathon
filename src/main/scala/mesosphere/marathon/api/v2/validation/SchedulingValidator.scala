package mesosphere.marathon
package api.v2.validation

import java.util.regex.Pattern

import com.wix.accord._
import com.wix.accord.dsl._
import mesosphere.marathon.api.v2.Validation
import mesosphere.marathon.raml.{ Constraint, PodPlacementPolicy, PodSchedulingBackoffStrategy, PodSchedulingPolicy, PodUpgradeStrategy, UpgradeStrategy }
import mesosphere.marathon.state.ResourceRole

import scala.util.Try

trait SchedulingValidator {
  import Validation._

  val backoffStrategyValidator = validator[PodSchedulingBackoffStrategy] { bs =>
    bs.backoff should be >= 0.0
    bs.backoffFactor should be >= 0.0
    bs.maxLaunchDelay should be >= 0.0
  }

  /** changes here should be reflected in [[appUpgradeStrategyValidator]] */
  val upgradeStrategyValidator = validator[PodUpgradeStrategy] { us =>
    us.maximumOverCapacity should be >= 0.0
    us.maximumOverCapacity should be <= 1.0
    us.minimumHealthCapacity should be >= 0.0
    us.minimumHealthCapacity should be <= 1.0
  }

  /** changes here should be reflected in [[upgradeStrategyValidator]] */
  implicit val appUpgradeStrategyValidator = validator[UpgradeStrategy] { us =>
    // TODO(jdef) consolidate pod and app RAML upgrade-strategy definitions
    us.maximumOverCapacity should optional(be >= 0.0)
    us.maximumOverCapacity should optional(be <= 1.0)
    us.minimumHealthCapacity should optional(be >= 0.0)
    us.minimumHealthCapacity should optional(be <= 1.0)
  }

  val complyWithConstraintRules: Validator[Constraint] = new Validator[Constraint] {
    import mesosphere.marathon.raml.ConstraintOperator._
    override def apply(c: Constraint): Result = {
      if (c.fieldName.isEmpty) {
        Failure(Set(RuleViolation(c, "Missing field and operator", None)))
      } else {
        c.operator match {
          case Unique =>
            c.value.fold[Result](Success) { _ => Failure(Set(RuleViolation(c, "Value specified but not used", None))) }
          case Cluster =>
            if (c.value.isEmpty || c.value.map(_.length).getOrElse(0) == 0) {
              Failure(Set(RuleViolation(c, "Missing value", None)))
            } else {
              Success
            }
          case GroupBy | MaxPer =>
            if (c.value.fold(true)(i => Try(i.toInt).isSuccess)) {
              Success
            } else {
              Failure(Set(RuleViolation(
                c,
                "Value was specified but is not a number",
                Some("GROUP_BY may either have no value or an integer value"))))
            }
          case Like | Unlike =>
            c.value.fold[Result] {
              Failure(Set(RuleViolation(c, "A regular expression value must be provided", None)))
            } { p =>
              Try(Pattern.compile(p)) match {
                case util.Success(_) => Success
                case util.Failure(e) =>
                  Failure(Set(RuleViolation(
                    c,
                    s"'$p' is not a valid regular expression",
                    Some(s"$p\n${e.getMessage}"))))
              }
            }
        }
      }
    }
  }

  val placementStrategyValidator = validator[PodPlacementPolicy] { ppp =>
    ppp.acceptedResourceRoles.toSet is empty or ResourceRole.validAcceptedResourceRoles(false) // TODO(jdef) assumes pods!! change this to properly support apps
    ppp.constraints is empty or every(complyWithConstraintRules)
  }

  val schedulingValidator = validator[PodSchedulingPolicy] { psp =>
    psp.backoff is optional(backoffStrategyValidator)
    psp.upgrade is optional(upgradeStrategyValidator)
    psp.placement is optional(placementStrategyValidator)
  }
}

object SchedulingValidator extends SchedulingValidator
