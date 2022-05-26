
from setuptools import setup, find_namespace_packages

DEPENDENCIES = [
    "ipaddress==1.0.22",
    "ujson==5.2.0",
    "pyzmq==17.1.2",
    "redis==2.10.6",
    "tornado==5.1.1",
    "pycryptodome==3.6.6",
    "mysql-connector-python==8.0.12",
    "GitPython==3.1.1",
    "idna==2.9",
    "Sphinx==3.4.3",
    "pyOpenSSL==18.0.0",
    "cffi==1.14.4",
    "cryptography==2.3.1",
    "expiringdict==1.1.4",
    "psutil==5.6.6",
    "lazy==1.3",
    "pympler==0.9",
    "sprockets-influxdb==2.2.1",
    "aioredis==1.3.1",
    "pika==0.12.0",
    "anthill-PyMySQL==0.9.999",
    "anthill-tormysql==0.4.0",
    "PyJWT==1.6.4"
]

setup(
    name='anthill-common',
    version='0.2.6',
    package_data={
      "anthill.common": ["anthill/common/sql", "anthill/common/static"]
    },
    description='Common utils for Anthill Platform',
    author='desertkun',
    license='MIT',
    author_email='desertkun@gmail.com',
    url='https://github.com/anthill-platform/anthill-common',
    namespace_packages=["anthill"],
    include_package_data=True,
    packages=find_namespace_packages(include=["anthill.*"]),
    zip_safe=False,
    install_requires=DEPENDENCIES
)
