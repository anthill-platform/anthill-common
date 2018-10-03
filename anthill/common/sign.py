
import abc
from cryptography.hazmat.backends.openssl.backend import backend
from cryptography.hazmat.primitives import serialization


TOKEN_SIGNATURE_RSA = 'RS256'
TOKEN_SIGNATURE_HMAC = 'HS256'


class AccessTokenSignature(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    @abc.abstractmethod
    def id(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def sign_key(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def validate_key(self):
        raise NotImplementedError()


class RSAAccessTokenSignature(AccessTokenSignature):
    def __init__(self, private_key=None, password=None, public_key=None):
        AccessTokenSignature.__init__(self)

        if private_key:
            with open(private_key, "rb") as f:
                self.private = serialization.load_pem_private_key(
                    f.read(),
                    password=password.encode(),
                    backend=backend)
        else:
            self.private = None
        with open(public_key, "rb") as f:
            self.public = serialization.load_pem_public_key(
                f.read(),
                backend=backend)

    def id(self):
        return TOKEN_SIGNATURE_RSA

    def sign_key(self):
        return self.private

    def validate_key(self):
        return self.public


class HMACAccessTokenSignature(AccessTokenSignature):
    def __init__(self, key=None):
        AccessTokenSignature.__init__(self)
        self.key = key

    def id(self):
        return TOKEN_SIGNATURE_HMAC

    def sign_key(self):
        return self.key

    def validate_key(self):
        return self.key
