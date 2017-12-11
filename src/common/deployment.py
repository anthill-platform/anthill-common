from tornado.gen import coroutine, Return

from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
from subprocess import call, CalledProcessError

from source import PrivateSSHKeyContext
from options import options

import os
import shutil
import tempfile

"""
These classes allow to 'deploy' any file to certain sources (either local file systems or CDN)
"""


class DeploymentError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class DeploymentMethod(object):
    @coroutine
    def deploy(self, gamespace_id, source_file_path, target_directory_name, target_file_name):
        """
        Performs the actual deployment of the file onto the target system
        :param gamespace_id: ID of the gamespace
        :param source_file_path: A path to existing file that should be deployed to the target system
        :param target_directory_name: Directory name of the file to be uploaded to
        :param target_file_name: Name for the target file in the target directory
        :return:
        """

        raise NotImplementedError()

    def dump(self):
        """
        Saves this deployment method object into a JSON object
        """
        return {}

    def load(self, data):
        """
        Loads a JSON object (data) into this deployment method object
        :return:
        """
        pass

    @staticmethod
    def render(a):
        """
        Returns administrative fields for visual editing in admin tool (see admin.py)
        """
        return {}

    @coroutine
    def update(self, **fields):
        """
        Updates properties of this deployment method from data sent by the user in admin tool
        """
        pass

    @staticmethod
    def has_admin():
        """
        :return: True if this deployment method can be configured in admin tool (there are options to tune)
        """
        return False


class LocalDeploymentMethod(DeploymentMethod):
    executor = ThreadPoolExecutor(max_workers=4)

    @run_on_executor
    def deploy(self, gamespace_id, source_file_path, target_directory_name, target_file_name):
        target_dir = os.path.join(options.data_runtime_location, target_directory_name)

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        shutil.copyfile(
            source_file_path,
            os.path.join(options.data_runtime_location, target_directory_name, target_file_name))

        return options.data_host_location + os.path.join(target_directory_name, target_file_name)


class KeyCDNDeploymentMethod(DeploymentMethod):
    executor = ThreadPoolExecutor(max_workers=4)

    KEYCDN_RSYNC_URL = "rsync.keycdn.com"

    def __init__(self):
        super(KeyCDNDeploymentMethod, self).__init__()

        self.pri = None
        self.url = None
        self.login = None
        self.zone = None
        self.directory = ""

    @staticmethod
    def render(a):
        return {
            "login": a.field("KeyCDN Username", "text", "primary", order=1),
            "zone": a.field("KeyCDN Zone Name", "text", "primary", order=2),
            "url": a.field("Public URL (including scheme)", "text", "primary", order=3),
            "directory": a.field("Directory to deliver files in",
                                 "text", "primary", order=3),
            "pri": a.field("Private SSH Key", "text", "primary", multiline=20, order=4)
        }

    @staticmethod
    def has_admin():
        return True

    @run_on_executor
    def deploy(self, gamespace_id, source_file_path, target_directory_name, target_file_name):

        with PrivateSSHKeyContext(self.pri) as key_path:
            try:
                mk_args = [
                    key_path,
                    self.login,
                    KeyCDNDeploymentMethod.KEYCDN_RSYNC_URL,
                    self.zone,
                    os.path.join(self.directory, target_directory_name, "")
                ]

                # rsync cannot upload a file to a directory that does not exists, so
                # use this dirty hack to create the directory beforehand
                return_code = call(
                    ["rsync -avz --no-motd -e 'ssh -i {0} -o StrictHostKeyChecking=no' "
                     "/dev/null {1}@{2}:zones/{3}/{4}".format(*mk_args)], shell=True)

                if not return_code:
                    args = [
                        key_path,
                        source_file_path,
                        self.login,
                        KeyCDNDeploymentMethod.KEYCDN_RSYNC_URL,
                        self.zone,
                        os.path.join(self.directory, target_directory_name, target_file_name)
                    ]

                    return_code = call(
                        ["rsync -rvz --no-motd "
                         "-e 'ssh -i {0} -o StrictHostKeyChecking=no' "
                         "{1} {2}@{3}:zones/{4}/{5}".format(*args)], shell=True)

            except CalledProcessError as e:
                raise DeploymentError("Rsync failed with code: " + str(e.returncode))
            except BaseException as e:
                raise DeploymentError(str(e))

            if return_code:
                raise DeploymentError("Rsync failed with code: " + str(return_code))

        return self.url + "/" + os.path.join(self.directory, target_directory_name, target_file_name)

    @coroutine
    def update(self, pri, url, login, zone, directory, **fields):
        self.pri = str(pri)
        self.url = str(url)
        self.login = str(login)
        self.zone = str(zone)
        self.directory = str(directory)

    def load(self, data):
        self.pri = data.get("pri")
        self.url = data.get("url")
        self.login = data.get("login")
        self.zone = data.get("zone")
        self.directory = data.get("directory", "")

    def dump(self):
        return {
            "pri": str(self.pri),
            "url": str(self.url),
            "login": str(self.login),
            "zone": str(self.zone),
            "directory": str(self.directory)
        }


class DeploymentMethods(object):
    METHODS = {
        "local": LocalDeploymentMethod,
        "keycdn": KeyCDNDeploymentMethod
    }

    @staticmethod
    def valid(method):
        return method in DeploymentMethods.METHODS

    @staticmethod
    def types():
        return DeploymentMethods.METHODS.keys()

    @staticmethod
    def get(method):
        return DeploymentMethods.METHODS.get(method)
