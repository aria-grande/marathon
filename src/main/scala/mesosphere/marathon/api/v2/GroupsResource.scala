package mesosphere.marathon
package api.v2

import java.net.URI
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, Response }

import com.codahale.metrics.annotation.Timed
import mesosphere.marathon.api.v2.InfoEmbedResolver._
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.api.v2.json.GroupUpdate
import mesosphere.marathon.api.{ AuthResource, MarathonMediaType }
import mesosphere.marathon.core.appinfo.{ GroupInfo, GroupInfoService, Selector }
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.raml.{ GroupConversion, Raml }
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state._
import mesosphere.marathon.stream._
import mesosphere.marathon.upgrade.DeploymentPlan
import play.api.libs.json.Json

import scala.concurrent.Future

@Path("v2/groups")
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class GroupsResource @Inject() (
    groupManager: GroupManager,
    infoService: GroupInfoService,
    val config: MarathonConf)(implicit
  val authenticator: Authenticator,
    val authorizer: Authorizer) extends AuthResource {

  import GroupsResource._

  /**
    * For backward compatibility, we embed always apps, pods, and groups if nothing is specified.
    */
  val defaultEmbeds = Set(EmbedApps, EmbedPods, EmbedGroups)

  /**
    * Path matchers. Needed since Jersey is not able to handle parameters with slashes.
    */
  val ListApps = """^((?:.+/)|)apps$""".r
  val ListRootApps = """^apps$""".r
  val ListVersionsRE = """^(.+)/versions$""".r
  val ListRootVersionRE = """^versions$""".r
  val GetVersionRE = """^(.+)/versions/(.+)$""".r
  val GetRootVersionRE = """^versions/(.+)$""".r

  /*
  implicit lazy val GroupFormat: Format[Group] = (
    (__ \ "id").format[PathId] ~
    (__ \ "apps").formatNullable[Iterable[AppDefinition]].withDefault(Iterable.empty) ~
    (__ \ "pods").formatNullable[Iterable[Pod]].withDefault(Iterable.empty) ~
    (__ \ "groups").lazyFormatNullable(implicitly[Format[Iterable[Group]]]).withDefault(Iterable.empty) ~
    (__ \ "dependencies").formatNullable[Set[PathId]].withDefault(Group.defaultDependencies) ~
    (__ \ "version").formatNullable[Timestamp].withDefault(Group.defaultVersion)
  ) (
      (id, apps, pods, groups, dependencies, version) =>
        Group(id = id, apps = apps.map(app => app.id -> app)(collection.breakOut),
          pods.map(p => PathId(p.id).canonicalPath() -> Raml.fromRaml(p))(collection.breakOut),
          groupsById = groups.map(group => group.id -> group)(collection.breakOut),
          dependencies = dependencies, version = version),
      { (g: Group) => (g.id, g.apps.values, g.pods.values.map(Raml.toRaml(_)), g.groups, g.dependencies, g.version) })
  */

  /**
    * Get root group.
    */
  @GET
  @Timed
  def root(@Context req: HttpServletRequest, @QueryParam("embed") embed: java.util.Set[String]): Response =
    group("/", embed, req)

  /**
    * Get a specific group, optionally with specific version
    * @param id the identifier of the group encoded as path
    * @return the group or the group versions.
    */
  @GET
  @Path("""{id:.+}""")
  @Timed
  def group(
    @PathParam("id") id: String,
    @QueryParam("embed") embed: java.util.Set[String],
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    import scala.concurrent.ExecutionContext.Implicits.global

    val embeds: Set[String] = if (embed.isEmpty) defaultEmbeds else embed
    val (appEmbed, groupEmbed) = resolveAppGroup(embeds)

    //format:off
    def appsResponse(id: PathId) =
      infoService.selectAppsInGroup(id, authorizationSelectors.appSelector, appEmbed).map(info => ok(info))

    def groupResponse(id: PathId) =
      infoService.selectGroup(id, authorizationSelectors, appEmbed, groupEmbed).map {
        case Some(info) => ok(info)
        case None if id.isRoot => ok(GroupInfo.empty)
        case None => unknownGroup(id)
      }

    def groupVersionResponse(id: PathId, version: Timestamp) =
      infoService.selectGroupVersion(id, version, authorizationSelectors, groupEmbed).map {
        case Some(info) => ok(info)
        case None => unknownGroup(id)
      }

    def versionsResponse(groupId: PathId) = {
      groupManager.group(groupId).map { maybeGroup =>
        withAuthorization(ViewGroup, maybeGroup, unknownGroup(groupId)) { _ =>
          result(groupManager.versions(groupId).map(versions => ok(versions)))
        }
      }
    }

    val response: Future[Response] = id match {
      case ListApps(gid) => appsResponse(gid.toRootPath)
      case ListRootApps() => appsResponse(PathId.empty)
      case ListVersionsRE(gid) => versionsResponse(gid.toRootPath)
      case ListRootVersionRE() => versionsResponse(PathId.empty)
      case GetVersionRE(gid, version) => groupVersionResponse(gid.toRootPath, Timestamp(version))
      case GetRootVersionRE(version) => groupVersionResponse(PathId.empty, Timestamp(version))
      case _ => groupResponse(id.toRootPath)
    }

    result(response)
  }

  /**
    * Create a new group.
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    * @param body the request body as array byte buffer
    */
  @POST
  @Timed
  def create(
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    body: Array[Byte],
    @Context req: HttpServletRequest): Response = createWithPath("", force, body, req)

  def normalize(update: raml.GroupUpdate): raml.GroupUpdate = {
    // TODO(jdef): recursion without tailrec
    // would be nice if raml-generator used Set instead of Seq for apps and groups properties, then we could avoid this
    // see RAML 1.0 spec's uniqueItems property (semantics are MUST be unique items).

    // for now, enforce uniqueness this way (old GroupUpdate used Set)
    val groups = update.groups.map(_.map(normalize))
    update.copy(groups = groups)
  }

  /**
    * Create a group.
    * If the path to the group does not exist, it gets created.
    * @param id is the identifier of the the group to update.
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    * @param body the request body as array byte buffer
    */
  @POST
  @Path("""{id:.+}""")
  @Timed
  def createWithPath(
    @PathParam("id") id: String,
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    body: Array[Byte],
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    val raw = Json.parse(body).as[raml.GroupUpdate]
    withValid(normalize(raw)){ groupUpdate =>
      val effectivePath = groupUpdate.id.map(PathId(_).canonicalPath(id.toRootPath)).getOrElse(id.toRootPath)
      val rootGroup = result(groupManager.rootGroup())

      def throwIfConflicting[A](conflict: Option[Any], msg: String) = {
        conflict.map(_ => throw ConflictingChangeException(msg))
      }

      throwIfConflicting(
        rootGroup.findGroup(_.id == effectivePath),
        s"Group $effectivePath is already created. Use PUT to change this group.")
      throwIfConflicting(
        rootGroup.transitiveAppsById.get(effectivePath),
        s"An app with the path $effectivePath already exists.")

      val (deployment, path) = updateOrCreate(id.toRootPath, groupUpdate, force)
      deploymentResult(deployment, Response.created(new URI(path.toString)))
    }(GroupUpdate.validNestedGroupUpdateWithBase(id.toRootPath, config.availableFeatures))
  }

  @PUT
  @Timed
  def updateRoot(
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @DefaultValue("false")@QueryParam("dryRun") dryRun: Boolean,
    body: Array[Byte],
    @Context req: HttpServletRequest): Response = {
    update("", force, dryRun, body, req)
  }

  /**
    * Create or update a group.
    * If the path to the group does not exist, it gets created.
    * @param id is the identifier of the the group to update.
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    * @param dryRun only create the deployment without executing it.
    */
  @PUT
  @Path("""{id:.+}""")
  @Timed
  def update(
    @PathParam("id") id: String,
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @DefaultValue("false")@QueryParam("dryRun") dryRun: Boolean,
    body: Array[Byte],
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>

    val raw = Json.parse(body).as[raml.GroupUpdate]
    withValid(normalize(raw)) { groupUpdate =>
      val newVersion = Timestamp.now()

      if (dryRun) {
        val group = result(groupManager.group(id.toRootPath)).getOrElse(Group.empty)

        val originalGroup = Group.empty.update(group.id, _ => group, Group.defaultVersion)
        val updatedGroup = applyGroupUpdate(originalGroup, groupUpdate, newVersion)

        ok(
          Json.obj(
            "steps" -> DeploymentPlan(originalGroup, updatedGroup).steps
          ).toString()
        )
      } else {
        val (deployment, _) = updateOrCreate(id.toRootPath, groupUpdate, force)
        deploymentResult(deployment)
      }
    }(GroupUpdate.validNestedGroupUpdateWithBase(id.toRootPath, config.availableFeatures))
  }

  @DELETE
  @Timed
  def delete(
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    def clearRootGroup(rootGroup: Group): Group = {
      checkAuthorization(DeleteGroup, rootGroup)
      rootGroup.copy(apps = Map.empty, pods = Map.empty, groupsById = Map.empty)
    }

    val deployment = result(groupManager.update(
      PathId.empty,
      clearRootGroup,
      Timestamp.now(),
      force
    ))
    deploymentResult(deployment)
  }

  /**
    * Delete a specific subtree or a complete tree.
    * @param id the identifier of the group to delete encoded as path
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    * @return A version response, which defines the resulting change.
    */
  @DELETE
  @Path("""{id:.+}""")
  @Timed
  def delete(
    @PathParam("id") id: String,
    @DefaultValue("false")@QueryParam("force") force: Boolean,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val groupId = id.toRootPath
    val version = Timestamp.now()

    def deleteGroup(parentGroup: Group) = {
      parentGroup.group(groupId) match {
        case Some(group) => checkAuthorization(DeleteGroup, group)
        case None => throw UnknownGroupException(groupId)
      }
      parentGroup.remove(groupId, version)
    }

    val deployment = result(groupManager.update(groupId.parent, deleteGroup, version, force))
    deploymentResult(deployment)
  }

  private def applyGroupUpdate(
    group: Group,
    groupUpdate: raml.GroupUpdate,
    newVersion: Timestamp)(implicit identity: Identity) = {
    def versionChange = groupUpdate.version.map { version =>
      val targetVersion = Timestamp(version)
      checkAuthorization(UpdateGroup, group)
      val versionedGroup = result(groupManager.group(group.id, targetVersion))
        .map(checkAuthorization(ViewGroup, _))
        .map(_.update(group.id, Predef.identity, newVersion))
      versionedGroup.getOrElse(
        throw new IllegalArgumentException(s"Group $group.id not available in version $targetVersion")
      )
    }

    def scaleChange = groupUpdate.scaleBy.map { scale =>
      checkAuthorization(UpdateGroup, group)
      group.updateApps(newVersion) { app => app.copy(instances = (app.instances * scale).ceil.toInt) }
    }

    def createOrUpdateChange = {
      // groupManager.update always passes a group, even if it doesn't exist
      val maybeExistingGroup = result(groupManager.group(group.id))
      val updatedGroup: Group = Raml.fromRaml(
        GroupConversion.UpdateGroupStructureOp(groupUpdate, group, newVersion) -> appsResourceContext)

      maybeExistingGroup match {
        case Some(existingGroup) => checkAuthorization(UpdateGroup, existingGroup)
        case None => checkAuthorization(CreateRunSpec, updatedGroup)
      }

      updatedGroup
    }

    versionChange orElse scaleChange getOrElse createOrUpdateChange
  }

  private def updateOrCreate(
    id: PathId,
    update: raml.GroupUpdate,
    force: Boolean)(implicit identity: Identity): (DeploymentPlan, PathId) = {
    val version = Timestamp.now()

    val effectivePath = update.id.map(PathId(_).canonicalPath(id)).getOrElse(id)
    val deployment = result(groupManager.update(effectivePath, applyGroupUpdate(_, update, version), version, force))
    (deployment, effectivePath)
  }

  def authorizationSelectors(implicit identity: Identity): GroupInfoService.Selectors = {
    GroupInfoService.Selectors(
      AppsResource.authzSelector,
      PodsResource.authzSelector,
      authzSelector)
  }
}

object GroupsResource {

  val appsResourceContext: GroupConversion.Context = AppsResourceContext

  def authzSelector(implicit authz: Authorizer, identity: Identity) = Selector[Group] { g =>
    authz.isAuthorized(identity, ViewGroup, g)
  }

  object AppsResourceContext extends GroupConversion.Context {
    override def preprocess(app: raml.App): AppDefinition = Raml.fromRaml(AppsResource.preprocess(app))
  }
}
