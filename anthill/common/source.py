
from tornado.gen import Future, Task
from tornado.ioloop import IOLoop
from concurrent.futures import ThreadPoolExecutor

from git import Repo, Git, GitError, InvalidGitRepositoryError

from . import run_on_executor
from . database import DatabaseError
from . validate import validate, ValidationError

import os
import logging
from shutil import rmtree
from tempfile import mkstemp


class SourceCodeError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


EXECUTOR = ThreadPoolExecutor(4)


class ProjectBuild(object):
    executor = EXECUTOR

    def __init__(self, commit, build_dir, project):
        self.project = project

        self.commit = commit
        self.build_dir = build_dir
        self._inited = Future()

    async def init(self):
        """
        Should be awaited once ProjectVersionBuild is retrieved. Notifies that build is ready to use.
        """
        future = await Task(self._init)
        return future.result()

    def _init(self, callback):
        IOLoop.current().add_future(self._inited, callback)

    async def __do_setup__(self):
        try:
            await self.__setup__()
        except Exception as e:
            self._inited.set_exception(e)
        else:
            self._inited.set_result(True)

    async def __setup__(self):
        if not os.path.isdir(self.build_dir):
            await self.__checkout__()

    @run_on_executor
    def __checkout__(self):
        os.mkdir(self.build_dir)

        try:
            try:
                working_dir = os.path.abspath(self.project.repo_dir)
                g = Git(working_dir)
            except GitError as e:
                raise SourceCodeError(500, e.__class__.__name__ + ": " + str(e))

            with PrivateSSHKeyContext(ssh_private_key=self.project.ssh_private_key) as ssh_private_key_filename:
                with git_ssh_environment(g, ssh_private_key_filename=ssh_private_key_filename):
                    logging.info("Checking if the commit {0} into repo {1} exists".format(
                        self.commit,
                        self.project.remote_url
                    ))

                    try:
                        exists = g.cat_file("-t", self.commit) == "commit"
                    except GitError as e:
                        # noinspection PyUnresolvedReferences
                        if e.status == 128:
                            exists = False
                        else:
                            raise SourceCodeError(500, e.__class__.__name__ + ": " + str(e))

                    if not exists:
                        logging.info("No such commit: {0}, trying to update the repo {1}".format(
                            self.commit,
                            self.project.remote_url
                        ))

                        try:
                            up_to_date = "up-to-date" in g(work_tree=working_dir).pull()
                        except GitError as e:
                            raise SourceCodeError(500, e.__class__.__name__ + ": " + str(e))

                        if up_to_date:
                            raise SourceCodeError(404, "No such commit in the repo {0}: {1}".format(
                                self.project.remote_url,
                                self.commit
                            ))

                        logging.info("Updated, checking if the commit {0} into repo {1} exists again".format(
                            self.commit,
                            self.project.remote_url
                        ))

                        try:
                            exists = g.cat_file("-t", self.commit) == "commit"
                        except GitError as e:
                            # noinspection PyUnresolvedReferences
                            if e.status == 128:
                                exists = False
                            else:
                                raise SourceCodeError(500, e.__class__.__name__ + ": " + str(e))

                        if not exists:
                            raise SourceCodeError(404, "No such commit in the repo {0}: {1}".format(
                                self.project.remote_url,
                                self.commit
                            ))

                    logging.info("Checking out repo {0} into {1} (commit {2})".format(
                        self.project.remote_url,
                        self.build_dir,
                        self.commit
                    ))

                    try:
                        g(work_tree=os.path.abspath(self.build_dir)).checkout(self.commit, "--", ".")
                    except GitError as e:
                        raise SourceCodeError(500, e.__class__.__name__ + ": " + str(e))

        except Exception as e:
            rmtree(self.build_dir, ignore_errors=True)
            raise e


class PrivateSSHKeyContext(object):
    """
    This context manager class creates temporary file with ssh_private_key in it,
        and conveniently returns path to it, taking care to remove the file afterwards:

    with PrivateSSHKeyWrapper("private ssh key string") as key_path:
        use key_path here for ssh operations

    key_path deleted afterwards

    """

    def __init__(self, ssh_private_key=None):
        self.name = None
        self.ssh_private_key = ssh_private_key
        self.sys_fd = None

    @staticmethod
    def convert_path(path):
        separator = os.path.sep
        if separator != '/':
            path = path.replace(os.path.sep, '/')
        return path

    def __enter__(self):
        if self.ssh_private_key is None:
            return None

        self.sys_fd, self.name = mkstemp()

        with os.fdopen(self.sys_fd, 'w') as f:
            f.write(self.ssh_private_key)
            f.write("\n")

        return PrivateSSHKeyContext.convert_path(self.name)

    def __exit__(self, exc, value, tb):
        self.cleanup()

    def cleanup(self):
        if self.name is None:
            return

        try:
            os.close(self.sys_fd)
            os.remove(self.name)
        except IOError:
            pass


def git_ssh_environment(g, ssh_private_key_filename=None):
    if ssh_private_key_filename:
        return g.custom_environment(GIT_SSH_COMMAND=Project.git_ssh_command(ssh_private_key_filename))
    return g.custom_environment()


class SourceCodeRoot(object):
    DEFAULT_BRANCH = "master"

    executor = EXECUTOR

    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.projects = {}

        if not os.path.isdir(root_dir):
            os.makedirs(root_dir)

    @staticmethod
    def __get_project_key__(gamespace_id, project_name):
        return str(gamespace_id) + "_" + str(project_name)

    def project(self, gamespace_id, project_name, remote_url, branch_name=DEFAULT_BRANCH, ssh_private_key=""):
        project_key = SourceCodeRoot.__get_project_key__(gamespace_id, project_name)

        project = self.projects.get(project_key)
        if project:
            return project

        gamespace_dir = os.path.join(self.root_dir, str(gamespace_id))
        if not os.path.isdir(gamespace_dir):
            os.mkdir(gamespace_dir)

        project_dir = os.path.join(gamespace_dir, project_name)
        project = Project(project_dir, remote_url, branch_name=branch_name, ssh_private_key=ssh_private_key)
        self.projects[project_key] = project

        IOLoop.current().spawn_callback(project.__do_setup__)
        return project

    @run_on_executor
    @validate(url="str")
    def validate_repository_url(self, url, ssh_private_key=None):

        with PrivateSSHKeyContext(ssh_private_key) as ssh_private_key_filename:
            try:
                g = Git()
                with git_ssh_environment(g, ssh_private_key_filename=ssh_private_key_filename):
                    g.ls_remote(url)
            except GitError:
                return False
            else:
                return True


class Project(object):
    executor = EXECUTOR

    REPOSITORY_DIR = 'repo.git'
    PROJECT_BUILDS_DIR = 'builds'

    def __init__(self, project_dir, remote_url, branch_name=SourceCodeRoot.DEFAULT_BRANCH, ssh_private_key=None):
        self.project_dir = project_dir
        self.remote_url = remote_url
        self.branch_name = branch_name
        self.ssh_private_key = ssh_private_key
        self.builds = {}

        if not os.path.isdir(project_dir):
            os.makedirs(project_dir)

        self.repo_dir = os.path.join(self.project_dir, Project.REPOSITORY_DIR)
        self.builds_dir = os.path.join(self.project_dir, Project.PROJECT_BUILDS_DIR)

        if not os.path.isdir(self.builds_dir):
            os.mkdir(self.builds_dir)

        self.repo = None
        self._inited = Future()

    def build(self, commit):
        build = self.builds.get(commit)
        if build:
            return build

        build_dir = os.path.join(self.builds_dir, commit)
        build = ProjectBuild(commit, build_dir, self)
        IOLoop.current().spawn_callback(build.__do_setup__)
        return build

    async def init(self):
        """
        Should be awaited once ProjectRepository is retrieved. Notifies that repo is ready to use.
        """
        future = await Task(self._init)
        return future.result()

    def _init(self, callback):
        IOLoop.current().add_future(self._inited, callback)

    async def __do_setup__(self):
        try:
            await self.__setup__()
        except Exception as e:
            self._inited.set_exception(e)
        else:
            self._inited.set_result(True)

    async def __setup__(self):
        if not os.path.isdir(self.repo_dir):
            await self.__clone_repo__()

        try:
            self.repo = Repo(self.repo_dir)
        except InvalidGitRepositoryError:
            os.rmdir(self.repo_dir)
            result = await self.__setup__()
            return result
        except GitError as e:
            raise SourceCodeError(500, e.__class__.__name__ + ": " + str(e))

        # mark the ProjectRepository as ready
        return True

    @run_on_executor
    def get_commits_history(self, amount=20):
        try:
            return self.repo.iter_commits(self.branch_name, max_count=amount)
        except GitError as e:
            raise SourceCodeError(500, "Failed to list commit history: " + str(e.message))

    @run_on_executor
    def check_commit(self, commit):
        try:
            working_dir = os.path.abspath(self.repo_dir)
            g = Git(working_dir)
            with PrivateSSHKeyContext(ssh_private_key=self.ssh_private_key) as ssh_private_key_filename:
                with git_ssh_environment(g, ssh_private_key_filename=ssh_private_key_filename):
                    exists = g.cat_file("-t", commit) == "commit"
        except GitError:
            return False
        else:
            return exists

    @run_on_executor
    def pull_and_get_latest_commit(self):
        try:
            working_dir = os.path.abspath(self.repo_dir)
            g = Git(working_dir)
            with PrivateSSHKeyContext(ssh_private_key=self.ssh_private_key) as ssh_private_key_filename:
                with git_ssh_environment(g, ssh_private_key_filename=ssh_private_key_filename):
                    instance = g()
                    logging.info("Pulling updates from repo {0}".format(self.repo_dir))
                    instance.remote("update", "--prune")
                    return instance.log("-n", "1", self.branch_name, "--pretty=format:%H")
        except GitError:
            logging.exception("Failed to pull repo {0}".format(self.repo_dir))
            return None

    @run_on_executor
    def pull(self):
        try:
            working_dir = os.path.abspath(self.repo_dir)
            g = Git(working_dir)
            with PrivateSSHKeyContext(ssh_private_key=self.ssh_private_key) as ssh_private_key_filename:
                with git_ssh_environment(g, ssh_private_key_filename=ssh_private_key_filename):
                    instance = g()
                    logging.info("Pulling updates from repo {0}".format(self.repo_dir))
                    instance.remote("update", "--prune")
        except GitError:
            logging.exception("Failed to pull repo {0}".format(self.repo_dir))
            return False
        else:
            return True

    @staticmethod
    def git_ssh_command(private_key):
        if private_key is None:
            return "ssh"
        return "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {0}".format(private_key)

    @run_on_executor
    def __clone_repo__(self):
        logging.info("Cloning repository {0} into {1} branch {2} only".format(
            self.remote_url,
            self.repo_dir,
            self.branch_name
        ))

        with PrivateSSHKeyContext(ssh_private_key=self.ssh_private_key) as ssh_private_key_filename:
            env = {}

            if ssh_private_key_filename:
                env["GIT_SSH_COMMAND"] = Project.git_ssh_command(ssh_private_key_filename)

            Repo.clone_from(
                self.remote_url, self.repo_dir,
                branch=self.branch_name,
                single_branch=True,
                shallow_submodules=True,
                recurse_submodules=".",
                mirror=True,
                env=env)


class SourceCommitAdapter(object):
    def __init__(self, data):
        self.gamespace_id = data.get("gamespace_id")
        self.name = data.get("application_name")
        self.version = data.get("application_version")
        self.repository_commit = data.get("repository_commit")
        self.repository_url = data.get("repository_url")
        self.repository_branch = data.get("repository_branch")
        self.ssh_private_key = data.get("ssh_private_key")


class SourceProjectAdapter(object):
    def __init__(self, data):
        self.gamespace_id = data.get("gamespace_id")
        self.name = data.get("application_name")
        self.repository_url = data.get("repository_url")
        self.repository_branch = data.get("repository_branch")
        self.ssh_private_key = data.get("ssh_private_key")


class ServerCodeAdapter(object):
    def __init__(self, data):
        self.gamespace_id = data.get("gamespace_id")
        self.repository_url = data.get("repository_url")
        self.repository_branch = data.get("repository_branch")
        self.ssh_private_key = data.get("ssh_private_key")
        self.repository_commit = data.get("repository_commit")


class NoSuchSourceError(Exception):
    pass


class NoSuchProjectError(Exception):
    pass


# noinspection SqlResolve
class DatabaseSourceCodeRoot(object):
    executor = EXECUTOR

    def __init__(self, db, tables_prefix):
        self.db = db
        self.tables_prefix = tables_prefix
        self.rc_cache = {}

    @validate(gamespace_id="int", application_name="str", application_version="str", repository_commit="str")
    async def update_commit(self, gamespace_id, application_name, application_version, repository_commit):
        try:
            updated = await self.db.execute(
                """
                INSERT INTO `{0}_application_versions`
                (`gamespace_id`, `application_name`, `application_version`, `repository_commit`)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY 
                UPDATE `repository_commit`=VALUES(`repository_commit`);
                """.format(self.tables_prefix),
                gamespace_id, application_name, application_version, repository_commit
            )
        except DatabaseError as e:
            raise SourceCodeError(500, e.args[1])
        else:
            return updated

    @validate(gamespace_id="int", repository_commit="str")
    async def update_server_commit(self, gamespace_id, repository_commit):
        try:
            updated = await self.db.execute(
                """
                UPDATE `{0}_server`
                SET `repository_commit`=%s
                WHERE `gamespace_id`=%s
                LIMIT 1;
                """.format(self.tables_prefix),
                repository_commit, gamespace_id
            )
        except DatabaseError as e:
            raise SourceCodeError(500, e.args[1])
        else:
            return updated

    @validate(gamespace_id="int", application_name="str", application_version="str")
    async def delete_commit(self, gamespace_id, application_name, application_version):
        try:
            deleted = await self.db.execute(
                """
                DELETE FROM `{0}_application_versions`
                WHERE `gamespace_id`=%s AND `application_name`=%s AND `application_version`=%s
                LIMIT 1;
                """.format(self.tables_prefix),
                gamespace_id, application_name, application_version
            )
        except DatabaseError as e:
            raise SourceCodeError(500, e.args[1])
        else:
            return deleted

    @validate(gamespace_id="int")
    async def delete_server_commit(self, gamespace_id):
        try:
            deleted = await self.db.execute(
                """
                UPDATE `{0}_server`
                SET `repository_commit`=NULL 
                WHERE `gamespace_id`=%s
                LIMIT 1;
                """.format(self.tables_prefix),
                gamespace_id
            )
        except DatabaseError as e:
            raise SourceCodeError(500, e.args[1])
        else:
            return deleted

    @validate(gamespace_id="int", application_name="str")
    async def list_versions(self, gamespace_id, application_name):
        try:
            result = await self.db.query(
                """
                SELECT *
                FROM `{0}_application_versions` AS v
                WHERE v.`gamespace_id`=%s AND v.`application_name`=%s;
                """.format(self.tables_prefix), gamespace_id, application_name
            )
        except DatabaseError as e:
            raise SourceCodeError(500, e.args[1])

        return {
            item["application_version"]: SourceCommitAdapter(item)
            for item in result
        }

    @validate(gamespace_id="int", application_name="str", application_version="str")
    async def get_version_commit(self, gamespace_id, application_name, application_version):

        _key = "commits:" + str(gamespace_id) + ":" + str(application_name) + ":" + str(application_version)
        existing_futures = self.rc_cache.get(_key, None)

        if existing_futures is not None:
            future = Future()
            existing_futures.append(future)
            result = await future
            return result

        new_futures = []
        self.rc_cache[_key] = new_futures

        try:
            result = await self.db.get(
                """
                SELECT *
                FROM `{0}_application_settings` AS a,
                     `{0}_application_versions` AS v
                WHERE a.`gamespace_id`=%s AND a.`application_name`=%s AND 
                    v.`application_version`=%s AND v.`gamespace_id`=a.`gamespace_id` AND
                    v.`application_name`=a.`application_name`
                LIMIT 1;
                """.format(self.tables_prefix), gamespace_id, application_name, application_version
            )
        except DatabaseError as e:
            _e = SourceCodeError(500, e.args[1])
            for future in new_futures:
                future.set_exception(_e)
            del self.rc_cache[_key]
            raise _e

        if not result:
            e = NoSuchSourceError()
            for future in new_futures:
                future.set_exception(e)
            del self.rc_cache[_key]
            raise e

        adapter = SourceCommitAdapter(result)

        for future in new_futures:
            future.set_result(adapter)
        del self.rc_cache[_key]

        return adapter

    @validate(gamespace_id="int")
    async def get_server_commit(self, gamespace_id):

        _key = "server-commits:" + str(gamespace_id)
        existing_futures = self.rc_cache.get(_key, None)

        if existing_futures is not None:
            future = Future()
            existing_futures.append(future)
            result = await future
            return result

        new_futures = []
        self.rc_cache[_key] = new_futures

        try:
            result = await self.db.get(
                """
                SELECT *
                FROM `{0}_server` AS a
                WHERE a.`gamespace_id`=%s
                LIMIT 1;
                """.format(self.tables_prefix), gamespace_id
            )
        except DatabaseError as e:
            _e = SourceCodeError(500, e.args[1])
            for future in new_futures:
                future.set_exception(_e)
            del self.rc_cache[_key]
            raise _e

        if not result:
            e = NoSuchSourceError()
            for future in new_futures:
                future.set_exception(e)
            del self.rc_cache[_key]
            raise e

        adapter = ServerCodeAdapter(result)

        for future in new_futures:
            future.set_result(adapter)
        del self.rc_cache[_key]

        return adapter

    @validate(gamespace_id="int", application_name="str")
    async def get_project(self, gamespace_id, application_name):
        try:
            result = await self.db.get(
                """
                SELECT *
                FROM `{0}_application_settings` AS a
                WHERE a.`gamespace_id`=%s AND a.`application_name`=%s
                LIMIT 1;
                """.format(self.tables_prefix), gamespace_id, application_name
            )
        except DatabaseError as e:
            raise SourceCodeError(500, e.args[1])

        if not result:
            raise NoSuchProjectError()

        return SourceProjectAdapter(result)

    @validate(gamespace_id="int")
    async def get_server_project(self, gamespace_id):
        try:
            result = await self.db.get(
                """
                SELECT *
                FROM `{0}_server` AS a
                WHERE a.`gamespace_id`=%s
                LIMIT 1;
                """.format(self.tables_prefix), gamespace_id
            )
        except DatabaseError as e:
            raise SourceCodeError(500, e.args[1])

        if not result:
            raise NoSuchProjectError()

        return ServerCodeAdapter(result)

    @staticmethod
    def __validate_private_key__(ssh_private_key):
        if ssh_private_key:
            if ("BEGIN RSA PRIVATE KEY" not in ssh_private_key) or ("END RSA PRIVATE KEY" not in ssh_private_key):
                raise ValidationError("'ssh_private_key' appears to be corrupted.")

    @validate(gamespace_id="int", application_name="str", repository_url="str", repository_branch="str",
              ssh_private_key="str")
    async def update_project(self, gamespace_id, application_name, repository_url, repository_branch, ssh_private_key):

        DatabaseSourceCodeRoot.__validate_private_key__(ssh_private_key)

        if not repository_branch:
            raise ValidationError("'repository_branch' must not be empty")

        try:
            await self.db.execute(
                """
                INSERT INTO `{0}_application_settings`
                (`gamespace_id`, `application_name`, `repository_url`, `repository_branch`, `ssh_private_key`)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY 
                UPDATE `repository_url`=VALUES(`repository_url`), 
                       `repository_branch`=VALUES(`repository_branch`),
                       `ssh_private_key`=VALUES(`ssh_private_key`);
                """.format(self.tables_prefix),
                gamespace_id, application_name, repository_url, repository_branch, ssh_private_key
            )
        except DatabaseError as e:
            raise SourceCodeError(500, e.args[1])

    @validate(gamespace_id="int", repository_url="str", repository_branch="str", ssh_private_key="str")
    async def update_server_project(self, gamespace_id, repository_url, repository_branch, ssh_private_key):

        DatabaseSourceCodeRoot.__validate_private_key__(ssh_private_key)

        if not repository_branch:
            raise ValidationError("'repository_branch' must not be empty")

        try:
            await self.db.execute(
                """
                INSERT INTO `{0}_server`
                (`gamespace_id`, `repository_url`, `repository_branch`, `ssh_private_key`)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY 
                UPDATE `repository_url`=VALUES(`repository_url`), 
                       `repository_branch`=VALUES(`repository_branch`),
                       `ssh_private_key`=VALUES(`ssh_private_key`);
                """.format(self.tables_prefix),
                gamespace_id, repository_url, repository_branch, ssh_private_key
            )
        except DatabaseError as e:
            raise SourceCodeError(500, e.args[1])
