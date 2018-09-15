
import abc


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
    def sign_password(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def validate_key(self):
        raise NotImplementedError()


class RSAAccessTokenSignature(AccessTokenSignature):
    def __init__(self, private_key=None, password=None, public_key=None):
        AccessTokenSignature.__init__(self)

        if private_key:
            with open(private_key) as f:
                self.private = f.read()
        else:
            self.private = None
        self.password = password.encode() if password else None
        with open(public_key) as f:
            self.public = f.read()

    def id(self):
        return TOKEN_SIGNATURE_RSA

    def sign_key(self):
        return self.private

    def sign_password(self):
        return self.password

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

    def sign_password(self):
        return None

    def validate_key(self):
        return self.key
