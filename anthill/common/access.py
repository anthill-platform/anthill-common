
from tornado.gen import coroutine, Return, Task
from tornado.web import HTTPError

from . import keyvalue
from . import pubsub
from . options import options
from . internal import Internal, InternalError

import logging
import re
import calendar
import jwt



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
    return set(filter(lambda s: re.match(SCOPE_PATTERN, s), source.split(',')))


def validate_token_name(source):
    return re.match(TOKEN_NAME_PATTERN, source)


def utc_time():

    from datetime import datetime

    d = datetime.utcnow()
    return int(calendar.timegm(d.utctimetuple()))


def serialize_scopes(source):
    if isinstance(source, (list, set)):
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
        self.scopes = set()
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
        return set(scopes).issubset(self.scopes)

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
        if self.account and (not isinstance(self.account, str)):
            self.valid = False
            return False

        self.valid = True
        return True

    def time_left(self):
        return self.expiration_date - utc_time()

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

    async def __invalidate_uuid__(self, db, account, uuid):

        removed = await db.delete("id:" + str(uuid))
        if removed > 0:
            logging.info("Invalidated token '{0}' for account '{1}'".format(uuid, account))

    def acquire(self):
        return self.kv.acquire()

    async def get(self, account):
        async with self.kv.acquire() as db:
            return await db.get(account)

    async def load(self, application):
        self.subscriber = await application.acquire_subscriber()
        self.kv = keyvalue.KeyValueStorage(
            host=options.token_cache_host,
            port=options.token_cache_port,
            db=options.token_cache_db,
            max_connections=options.token_cache_max_connections)
        await self.subscribe()

    async def on_invalidate(self, data):
        try:
            account = data["account"]
            uuid = data["uuid"]
        except KeyError:
            logging.error("Bad message recevied to cache")
            return

        async with self.kv.acquire() as db:
            await self.__invalidate_uuid__(db, account, uuid)

    async def store(self, db, account, uuid, expire):
        await db.setex("id:" + uuid, expire, account)

    async def store_token(self, db, token):
        await self.store(db, token.account, token.uuid, token.expiration_date)

    async def store_token_no_db(self, token):
        async with self.kv.acquire() as db:
            await self.store_token(db, token)

    async def subscribe(self):
        await self.subscriber.handle(INVALIDATION_CHANNEL, self.on_invalidate)

    async def validate(self, token, db=None):
        if not isinstance(token, AccessToken):
            raise AttributeError("Argument 'token' is not an AccessToken")

        if not token.is_valid():
            return False

        if db:
            result = await self.validate_db(token, db=db)
            return result

        async with self.kv.acquire() as db:
            result = await self.validate_db(token, db=db)

        return result

    async def validate_db(self, token, db):
        uuid = token.uuid
        account = token.account

        issuer = token.get(AccessToken.ISSUER)

        # no issuer means no external validation
        if issuer is None:
            return True

        if await db.get("inv:" + uuid):
            return False

        db_account = await db.get("id:" + uuid)

        # keyvalue storage returns bytes, we have strings
        if db_account and db_account.decode() == account:
            return True

        try:
            await self.internal.request(issuer, "validate_token", access_token=token.key)
        except InternalError:
            await db.setex("inv:" + uuid, INVALID_TTL, "")
            return False
        else:
            expiration_date = int(token.get(AccessToken.EXPIRATION_DATE))
            now = int(utc_time())
            left = expiration_date - now
            if left > 0:
                await self.store(db, account, uuid, left)
                return True


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
        internal_ = Internal()

        ip = remote_ip(self.request)

        if not internal_.is_internal(ip):
            # attacker shouldn't even know this page exists
            raise HTTPError(404)

        return method(self, *args, **kwargs)
    return wrapper


def public():
    from . import sign

    return sign.RSAAccessTokenSignature(public_key=options.auth_key_public)


def private():
    from . import sign

    password = options.private_key_password

    return sign.RSAAccessTokenSignature(
        private_key=options.auth_key_private,
        password=password,
        public_key=options.auth_key_public)
