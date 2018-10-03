
from . import server, access
from . access import AccessToken

import jwt
from uuid import uuid4


MAX_TIME = 86400 * 60
ADMIN_TIME = 15 * 60


class AccessTokenGenerator(AccessToken):
    """
    Access token generator.

    Signs an access token using private key.
    Can be verified later using public key.
    """

    @staticmethod
    def generate(signer_id, requested_scopes, additional_containers, name=None,
                 uuid=None, max_time=None, token_only=False):

        for_time = max_time
        if for_time is None:
            for_time = MAX_TIME

        if any(scope_name.endswith("_admin") for scope_name in requested_scopes):
            for_time = min(ADMIN_TIME, for_time)

        if uuid is None:
            uuid = str(uuid4())

        if signer_id not in AccessToken.SIGNERS:
            raise server.ServerError("No such signer: '{0}'".format(signer_id))

        signer = AccessToken.SIGNERS[signer_id]
        now = int(access.utc_time())

        containers = {}

        if name is not None:
            containers[AccessToken.USERNAME] = name

        containers.update(additional_containers)

        containers.update({
            AccessToken.SCOPES: ",".join(requested_scopes),
            AccessToken.ISSUED_AT: str(now),
            AccessToken.EXPIRATION_DATE: str(now + for_time),
            AccessToken.UUID: uuid
        })

        access_token = jwt.encode(
            containers, signer.sign_key(),
            algorithm=signer_id)

        if token_only:
            return access_token

        result = {
            "expires": for_time,
            "account": containers.get(AccessToken.ACCOUNT),
            "key": access_token,
            "uuid": uuid,
            "scopes": requested_scopes
        }

        if name is not None:
            result["credential"] = name

        return result

    @staticmethod
    def refresh(signer_id, token, force=False):
        if not token.is_valid():
            raise server.ServerError("Token is not valid")
        if not force and not token.needs_refresh():
            raise server.ServerError("Token refresh is not necessary")

        return AccessTokenGenerator.generate(
            signer_id,
            token.scopes,
            token.fields,
            name=token.name,
            uuid=token.uuid)
