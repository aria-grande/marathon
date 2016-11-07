package mesosphere.marathon
package api.v2.json

import mesosphere.marathon.raml.{ AppConversion, Raml }
import mesosphere.marathon.state._

object AppUpdate {
  /*
  // The versionInfo may never be overridden by an AppUpdate.
  // Setting the version in AppUpdate means that the user wants to revert to that version. In that
  // case, we do not update the current AppDefinition but revert completely to the specified version.
  // For all other updates, the GroupVersioningUtil will determine a new version if the AppDefinition
  // has really changed.
  versionInfo = app.versionInfo,

  require(version.isEmpty || onlyVersionOrIdSet, "The 'version' field may only be combined with the 'id' field.")

  protected[api] def onlyVersionOrIdSet: Boolean = productIterator forall {
    case x: Some[Any] => x == version || x == id // linter:ignore UnlikelyEquality
    case _ => true
  }
  */

  def withoutPriorAppDefinition(update: raml.AppUpdate, appId: PathId): AppDefinition = {
    val selectedStrategy = AppConversion.ResidencyAndUpgradeStrategy(
      residency = update.residency.map(Raml.fromRaml(_)),
      upgradeStrategy = update.upgradeStrategy.map(Raml.fromRaml(_)),
      hasPersistentVolumes = update.container.exists(_.volumes.exists(_.persistent.nonEmpty)),
      hasExternalVolumes = update.container.exists(_.volumes.exists(_.external.nonEmpty))
    )
    val template = AppDefinition(
      appId, residency = selectedStrategy.residency, upgradeStrategy = selectedStrategy.upgradeStrategy)
    Raml.fromRaml((update -> template))
  }

  def withCanonizedIds(update: raml.AppUpdate, base: PathId = PathId.empty): raml.AppUpdate = update.copy(
    id = update.id.map(id => PathId(id).canonicalPath(base).toString),
    dependencies = update.dependencies.map(_.map(dep => PathId(dep).canonicalPath(base).toString))
  )


}
