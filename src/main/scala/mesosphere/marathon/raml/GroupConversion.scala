package mesosphere.marathon
package raml

import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.state.{ AppDefinition, PathId, Timestamp, Group => CoreGroup, VersionInfo => CoreVersionInfo }

trait GroupConversion {

  import GroupConversion._

  implicit val groupUpdateRamlReads: Reads[(UpdateGroupStructureOp, Context), CoreGroup] =
    Reads[(UpdateGroupStructureOp, Context), CoreGroup] { src =>
      val (op, context) = src
      op.apply(context)
    }
}

object GroupConversion extends GroupConversion {

  case class UpdateGroupStructureOp(
      update: GroupUpdate,
      current: CoreGroup,
      timestamp: Timestamp
  ) {
    import UpdateGroupStructureOp._

    require(update.scaleBy.isEmpty, "For a structural update, no scale should be given.")
    require(update.version.isEmpty, "For a structural update, no version should be given.")

    def apply(implicit ctx: Context): CoreGroup = {
      // TODO(jdef) validation should enforce that .apps and .groups contain distinct things (Set); RAML should specify `uniqueItems: true`

      val effectiveGroups = update.groups.fold(current.groupsById) { updates =>
        val currentIds = current.groupIds
        val groupIds = updates.map(groupId(_).canonicalPath(current.id)).toSet
        val changedIds = currentIds.intersect(groupIds)
        val changedIdList = changedIds.toList
        val groupUpdates = changedIdList
          .flatMap(gid => current.groupsById.get(gid))
          .zip(changedIdList.flatMap(gid => updates.find(groupId(_).canonicalPath(current.id) == gid)))
          .map { case (group, groupUpdate) => UpdateGroupStructureOp(groupUpdate, group, timestamp).apply }
        val groupAdditions = groupIds
          .diff(changedIds)
          .flatMap(gid => updates.find(groupId(_).canonicalPath(current.id) == gid))
          .map(update => toGroup(update, groupId(update).canonicalPath(current.id), timestamp))
        (groupUpdates.toSet ++ groupAdditions).map(group => group.id -> group).toMap
      }
      val effectiveApps: Map[ AppDefinition.AppKey, AppDefinition ] =
        update.apps.map(_.map(ctx.preprocess)).getOrElse(current.apps.values).map { currentApp =>
          val app = toApp(current.id, currentApp, timestamp)
          app.id -> app
        }(collection.breakOut)

      val effectiveDependencies = update.dependencies.fold(current.dependencies)(_.map(PathId(_).canonicalPath(current.id)).toSet)
      CoreGroup(current.id, effectiveApps, current.pods, effectiveGroups, effectiveDependencies, timestamp)
    }
  }

  object UpdateGroupStructureOp {

    def groupId(update: GroupUpdate): PathId = update.id.map(PathId(_)).getOrElse(
      // TODO this belongs in validation!
      throw new SerializationFailedException("No group id was given!")
    )

    def toApp(gid: PathId, app: AppDefinition, version: Timestamp): AppDefinition = {
      val appId = app.id.canonicalPath(gid)
      app.copy(id = appId, dependencies = app.dependencies.map(_.canonicalPath(gid)),
        versionInfo = CoreVersionInfo.OnlyVersion(version))
    }

    def toGroup(update: GroupUpdate, gid: PathId, version: Timestamp)(implicit ctx: Context): CoreGroup = CoreGroup(
      id = gid,
      apps = update.apps.getOrElse(Set.empty).map { currentApp =>
        val app = toApp(gid, ctx.preprocess(currentApp), version)
        app.id -> app
      }.toMap,
      pods = Map.empty[PathId, PodDefinition],
      groups = update.groups.getOrElse(Seq.empty)
        .map(sub => toGroup(sub, groupId(sub).canonicalPath(gid), version)).toSet,
      dependencies = update.dependencies.fold(Set.empty[PathId])(_.map(PathId(_).canonicalPath(gid)).toSet),
      version = version
    )
  }

  trait Context {
    def preprocess(app: App): AppDefinition
  }
}
