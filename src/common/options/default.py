
from . import define

# Internal

define("internal_restrict",
       default=["127.0.0.1/24", "::1/128"],
       help="An addresses considered internal (can be multiple). Requests from those are allowed to do everything, "
            "so adding public address is dangerous.",
       group="internal",
       multiple=True,
       type=str)

define("internal_broker",
       default="amqp://anthill:anthill@127.0.0.1:5672/dev",
       help="RabbitMQ messages broker location (amqp).",
       group="internal",
       type=str)

define("internal_max_connections",
       default=10,
       help="Maximum connections for internal broker (connection pool).",
       group="internal",
       type=int)

# Token cache

define("token_cache_host",
       default="127.0.0.1",
       help="Location of access token cache (redis).",
       group="token_cache",
       type=str)

define("token_cache_port",
       default=6379,
       help="Port of access token cache (redis).",
       group="token_cache",
       type=int)

define("token_cache_db",
       default="127.0.0.1",
       help="Database of access token cache (redis).",
       group="token_cache",
       type=int)

define("token_cache_max_connections",
       default=500,
       help="Maximum connections to the token cache (connection pool).",
       group="token_cache",
       type=int)

# Discovery

define("discovery_service",
       default="http://discovery-dev.anthill.internal",
       help="Discovery service location (if applicable).",
       group="discovery",
       type=str)

# Pub/sub

define("pubsub",
       default="amqp://anthill:anthill@127.0.0.1:5672/dev",
       help="Location of rabbitmq server for pub/sub operations.",
       type=str)

# Keys

define("auth_key_public",
       default="../anthill-keys/anthill.pub",
       help="Location of public key required for access token verification.",
       type=str)

# Static content

define("serve_static",
       default=True,
       help="Should service serve /static files or should it be done by reverse proxy",
       type=bool)

# Other

define("graceful_shutdown",
       default=True,
       help="Whether should service shutdown gracefully or not",
       type=bool)
