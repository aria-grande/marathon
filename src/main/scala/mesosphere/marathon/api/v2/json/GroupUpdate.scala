package mesosphere.marathon
package api.v2.json

import com.wix.accord._
import com.wix.accord.dsl._
import mesosphere.marathon.api.v2.Validation._
import mesosphere.marathon.api.v2.validation.AppValidation
import mesosphere.marathon.state.PathId

object GroupUpdate {

  def empty(id: PathId): raml.GroupUpdate = raml.GroupUpdate(Some(id.toString))

  /** requires that apps are in canonical form */
  def validNestedGroupUpdateWithBase(base: PathId, enabledFeatures: Set[String]): Validator[raml.GroupUpdate] =
    validator[raml.GroupUpdate] { group =>
      group is notNull

      group.version is theOnlyDefinedOptionIn(group)
      group.scaleBy is theOnlyDefinedOptionIn(group)

      group.id.map(PathId(_)) as "id" is optional(valid)
      group.apps is optional(every(
        AppValidation.validNestedApp(group.id.fold(base)(PathId(_).canonicalPath(base)), enabledFeatures)))
      group.groups is optional(every(
        validNestedGroupUpdateWithBase(group.id.fold(base)(PathId(_).canonicalPath(base)), enabledFeatures)))
    }

  def groupUpdateValid(enabledFeatures: Set[String]): Validator[raml.GroupUpdate] =
    validNestedGroupUpdateWithBase(PathId.empty, enabledFeatures)
}
