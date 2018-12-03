
from . import define

# Internal

define("api_version",
       default="0.2",
       help="Service API version to return to the users in header X-API-Version",
       type=str)

define("internal_restrict",
       default=["127.0.0.1/24", "::1/128"],
       help="An addresses considered internal (can be multiple). Requests from those are allowed to do everything, "
            "so adding public address is dangerous.",
       group="internal",
       multiple=True,
       type=str)

define("internal_broker",
       default="amqp://guest:guest@127.0.0.1:5672/",
       help="RabbitMQ messages broker location (amqp).",
       group="internal",
       type=str)

define("internal_max_connections",
       default=1,
       help="Maximum connections for internal broker (connection pool).",
       group="internal",
       type=int)

define("internal_channel_prefetch_count",
       default=1024,
       help="Channel prefetch for internal broker (how many a consumer may prefetch for processing).",
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
       default=9,
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
       default="http://localhost:9502",
       help="Discovery service location (if applicable).",
       group="discovery",
       type=str)

# Pub/sub

define("pubsub",
       default="amqp://guest:guest@127.0.0.1:5672/",
       help="Location of rabbitmq server for pub/sub operations.",
       type=str)

# Keys

define("auth_key_public",
       default="../.anthill-keys/anthill.pub",
       help="Location of public key required for access token verification.",
       type=str)

# Monitoring

define("enable_monitoring",
       default=False,
       help="Use monitoring or not.",
       group="monitoring",
       type=bool)

define("monitoring_host",
       default="127.0.0.1",
       help="Monitoring server location (InfluxDB).",
       group="monitoring",
       type=str)

define("monitoring_port",
       default=8086,
       help="Monitoring server port (InfluxDB).",
       group="monitoring",
       type=int)

define("monitoring_db",
       default="dev",
       help="Monitoring server database name (InfluxDB).",
       group="monitoring",
       type=str)

define("monitoring_username",
       default="",
       help="Monitoring server username name (InfluxDB).",
       group="monitoring",
       type=str)

define("monitoring_password",
       default="",
       help="Monitoring server password name (InfluxDB).",
       group="monitoring",
       type=str)

# Static content

define("serve_static",
       default=True,
       help="Should service serve /static files or should it be done by reverse proxy",
       type=bool)

# Other

define("debug",
       default=False,
       help="Is debug mode enabled (includes full stack trace)",
       type=bool)
