
from setuptools import setup, find_packages

DEPENDENCIES = [
    "ipaddress==1.0.22",
    "ujson==1.35",
    "pyzmq==17.1.2",
    "redis==2.10.6",
    "tornado==5.1.1",
    "pycryptodome==3.6.6",
    "mysql-connector-python==8.0.12",
    "GitPython==2.1.7",
    "tormysql==0.4.0",
    "Sphinx==1.8.1",
    "pyOpenSSL==18.0.0",
    "cffi==1.11.5",
    "cryptography==2.3.1",
    "expiringdict==1.1.4",
    "python-geoip-python3==1.3",
    "python-geoip-geolite2-yplan==2017.608",
    "psutil==5.4.7",
    "lazy==1.3",
    "pympler==0.6",
    "sprockets-influxdb==2.1.0",
    "aioredis==1.1.0",
    "pika==0.12.0",
    "PyMySQL==0.8.0",
    "PyJWT==1.6.1"
]

REPOS = [
    "git+https://github.com/anthill-utils/PyMySQL.git@0.8.0#egg=PyMySQL-0.8.0",
    "git+https://github.com/anthill-utils/pyjwt.git@1.6.1#egg=PyJWT-1.6.1"
]

setup(
    name='anthill-common',
    package_data={
      "anthill.common": ["anthill/common/sql", "anthill/common/static"]
    },
    setup_requires=["pypigit-version"],
    git_version="0.1.0",
    description='Common utils for Anthill platform',
    author='desertkun',
    license='MIT',
    author_email='desertkun@gmail.com',
    url='https://github.com/anthill-platform/anthill-common',
    namespace_packages=["anthill"],
    packages=find_packages(),
    zip_safe=False,
    install_requires=DEPENDENCIES,
    dependency_links=REPOS
)
