
import re
import calendar
import jwt

from tornado.web import HTTPError

import keyvalue
import pubsub
import logging

from tornado.gen import coroutine, Return, Task
from common.options import options
from internal import Internal, InternalError


ACCOUNT_PATTERN = re.compile("([a-z]+):(.+)")
SCOPE_PATTERN = re.compile("([a-z_]+)")
TOKEN_NAME_PATTERN = re.compile("([\w\-_]{1,64})")

INVALIDATION_CHANNEL = "INV"
INVALID_TTL = 30


def parse_account(source):

    if len(source) > 255:
        return None

    c = re.match(ACCOUNT_PATTERN, source)
    if c:
        return [c.group(1), c.group(2)]

    return None


def parse_scopes(source):
    source_scopes = filter(bool, source.split(','))
    res = []
    for s in source_scopes:
        if re.match(SCOPE_PATTERN, s):
            res.append(s)

    return res


def validate_token_name(source):
    return re.match(TOKEN_NAME_PATTERN, source)


def utc_time():

    from datetime import datetime

    d = datetime.utcnow()
    return int(calendar.timegm(d.utctimetuple()))


def serialize_scopes(source):
    if isinstance(source, list):
        return ",".join(source)

    return ""


class AccessToken:
    EXPIRATION_DATE = 'exp'
    ISSUED_AT = 'iat'
    SCOPES = 'sco'
    USERNAME = 'unm'
    GAMESPACE = 'gms'
    ISSUER = 'iss'
    UUID = 'uid'
    ACCOUNT = 'acc'

    MIN_EXP_TIME = 600
    AUTO_PROLONG_IN = 86400

    SIGNERS = {}

    # ----------------------------------------------------------------

    def __init__(self, key):
        self.key = key

        self.account = None
        self.name = None
        self.scopes = []
        self.uuid = None

        self.expiration_date = 0
        self.issued_at = 0

        self.valid = False
        self.fields = {}
        self.key_content = None

        self.validate()

    def get(self, field, default=None):
        return self.fields.get(field, default)

    def has_scope(self, scope):
        return scope in self.scopes

    def has_scopes(self, scopes):
        if scopes is None:
            return True
        for scope in scopes:
            if scope not in self.scopes:
                return False
        return True

    def is_valid(self):
        return self.valid

    @staticmethod
    def register_signer(signer):
        AccessToken.SIGNERS[signer.id()] = signer

    def set(self, field, data):
        self.fields[field] = data

    def validate(self):
        try:
            header = jwt.get_unverified_header(self.key)
        except jwt.DecodeError as e:
            self.valid = False
            return False

        if "alg" not in header:
            self.valid = False
            return False

        alg = header["alg"]

        if alg not in AccessToken.SIGNERS:
            self.valid = False
            return False

        signer = AccessToken.SIGNERS[alg]

        try:
            self.fields = jwt.decode(self.key, signer.validate_key(), algorithms=[alg])
        except jwt.ExpiredSignatureError as e:
            self.valid = False
            return False
        except jwt.InvalidTokenError as e:
            self.valid = False
            return False

        self.account = self.get(AccessToken.ACCOUNT)

        try:
            self.name = self.get(AccessToken.USERNAME)
            self.uuid = self.get(AccessToken.UUID)
            self.scopes = parse_scopes(self.get(AccessToken.SCOPES))

            self.expiration_date = int(self.get(AccessToken.EXPIRATION_DATE))
            self.issued_at = int(self.get(AccessToken.ISSUED_AT))
        except (KeyError, ValueError):
            self.valid = False
            return False

        # account may be empty
        if self.account and (not isinstance(self.account, (unicode, str))):
            self.valid = False
            return False

        self.valid = True
        return True

    def needs_refresh(self):
        if self.get(AccessToken.ISSUER) is None:
            return False

        now = utc_time()

        if now > self.expiration_date - AccessToken.MIN_EXP_TIME:
            return True

        if now > self.issued_at + AccessToken.AUTO_PROLONG_IN:
            return True

        return False

    @staticmethod
    def init(signers):
        for signer in signers:
            AccessToken.register_signer(signer)


class AccessTokenCache(object):
    def __init__(self):
        self.subscriber = None
        self.internal = Internal()
        self.handlers = {}
        self.kv = None

    @coroutine
    def __invalidate_uuid__(self, db, account, uuid):

        removed = yield Task(
            db.delete,
            "id:" + uuid)

        if removed > 0:
            logging.info("Invalidated token '{0}' for account '{1}'".format(uuid, account))

    def acquire(self):
        return self.kv.acquire()

    @coroutine
    def get(self, account):

        db = self.kv.acquire()
        try:

            result = yield Task(
                db.get,
                account)
        finally:
            yield db.release()

        raise Return(result)

    @coroutine
    def load(self):

        self.subscriber = pubsub.RabbitMQSubscriber(
            channels=[INVALIDATION_CHANNEL],
            name=options.name,
            broker=options.pubsub)

        self.kv = keyvalue.KeyValueStorage(
            host=options.token_cache_host,
            port=options.token_cache_port,
            db=options.token_cache_db,
            max_connections=options.token_cache_max_connections)

        yield self.subscribe()

    @coroutine
    def on_invalidate(self, data):

        try:
            account = data["account"]
            uuid = data["uuid"]
        except KeyError:
            logging.error("Bad message recevied to cache")
            return

        db = self.kv.acquire()
        try:
            yield self.__invalidate_uuid__(
                db,
                account,
                uuid)
        finally:
            yield db.release()

    @coroutine
    def release(self):
        yield self.subscriber.release()

    @coroutine
    def store(self, db, account, uuid, expire):
        yield Task(db.setex, "id:" + uuid, expire, account)

    @coroutine
    def store_token(self, db, token):
        yield self.store(
            db,
            token.account,
            token.uuid,
            token.expiration_date)

    @coroutine
    def subscribe(self):

        self.subscriber.handle(
            INVALIDATION_CHANNEL,
            self.on_invalidate)

        yield self.subscriber.start()

    @coroutine
    def validate(self, token, db=None):

        if not isinstance(token, AccessToken):
            raise AttributeError("Argument 'token' is not an AccessToken")

        if not token.is_valid():
            raise Return(False)

        if db:
            result = yield self.validate_db(
                token,
                db=db)

            raise Return(result)

        db = self.kv.acquire()
        try:
            result = yield self.validate_db(
                token,
                db=db)
        finally:
            yield db.release()

        raise Return(result)

    @coroutine
    def validate_db(self, token, db):
        uuid = token.uuid
        account = token.account

        issuer = token.get(AccessToken.ISSUER)

        # no issuer means no external validation
        if issuer is None:
            raise Return(True)

        if (yield Task(db.get, "inv:" + uuid)):
            raise Return(False)

        db_account = yield Task(db.get, "id:" + uuid)
        if db_account == account:
            raise Return(True)

        try:
            yield self.internal.request(
                issuer,
                "validate_token",
                access_token=token.key)

        except InternalError:
            yield Task(
                db.setex,
                "inv:" + uuid,
                INVALID_TTL,
                "")

            raise Return(False)
        else:
            expiration_date = int(token.get(AccessToken.EXPIRATION_DATE))
            now = int(utc_time())
            left = expiration_date - now

            if left > 0:
                yield self.store(
                    db,
                    account,
                    uuid,
                    left)

                raise Return(True)


def scoped(scopes=None, method=None, **other):
    """
    Check if the user has access to the system.
    If the user doesn't have scopes listed, 403 Forbidden is raised.
    Using this without arguments basically means that user at least have valid access token.

    :param scopes: A list of scopes the user should have access to
    :param method: If defined, will be called instead of 403 Forbidden error (with arguments 'scopes')
    """

    def wrapper1(m):
        def wrapper2(self, *args, **kwargs):
            current_user = self.current_user
            if (not current_user) or (not current_user.token.has_scopes(scopes)):

                if method and hasattr(self, method):
                    getattr(self, method)(scopes=scopes, **other)
                    return
                else:
                    raise HTTPError(
                        403,
                        "Access denied ('{0}' required)".format(
                            ", ".join(scopes or []))
                        if scopes else "Access denied")

            return m(self, *args, **kwargs)
        return wrapper2
    return wrapper1


def remote_ip(request):
    real_ip = request.headers.get("X-Real-IP")
    return real_ip or request.remote_ip


def internal(method):
    def wrapper(self, *args, **kwargs):
        internal_ = self.application.internal

        ip = remote_ip(self.request)

        if not internal_.is_internal(ip):
            # attacker shouldn't even know this page exists
            raise HTTPError(404)

        return method(self, *args, **kwargs)
    return wrapper


def public():
    import sign

    return sign.RSAAccessTokenSignature(public_key=options.auth_key_public)


def private():
    import sign

    password = options.private_key_password

    return sign.RSAAccessTokenSignature(
        private_key=options.auth_key_private,
        password=password,
        public_key=options.auth_key_public)
