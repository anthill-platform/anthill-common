
from tornado.gen import coroutine, Return
from database import DatabaseError


class ClusterError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class NoClusterError(Exception):
    pass


class Cluster(object):
    """
    This object manages abstract cluster of players around some entity.
    This is useful when you would like to have a lot of players on same entity, but with each player
        attached to his tight group cluster (for example, 50 of them).

    A nice example would be Cluster Leaderboards. When you post to a cluster leaderboard you can only see
        records from your virtual group (cluster), and compete around them. Instead of having to compete
        around hundreds of thousands of players you have only 50 instead, virtually.

    See get_cluster for more info.
    """
    def __init__(self, db, table_name, accounts_table_name):
        """
        :param db: A database reference
        :param table_name: table name holding clusters themselves

        Scheme for it would be:

            CREATE TABLE `<table_name>` (
                `cluster_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                `gamespace_id` int(11) unsigned NOT NULL,
                `cluster_size` int(11) unsigned NOT NULL,
                `cluster_data` int(11) unsigned NOT NULL,
                PRIMARY KEY (`cluster_id`),
                KEY `leaderboard_id` (`cluster_data`)
            ) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

        :param accounts_table_name: table name holding cluster record (which player belong to whom)

        According scheme for it (please note a reference to <table_name>):

            CREATE TABLE `<accounts_table_name>` (
                `account_id` int(11) unsigned NOT NULL,
                `gamespace_id` int(11) unsigned NOT NULL,
                `cluster_id` int(11) unsigned NOT NULL,
                `cluster_data` int(11) unsigned NOT NULL,
                PRIMARY KEY (`account_id`,`gamespace_id`,`cluster_id`,`cluster_data`),
                UNIQUE KEY `account_id` (`account_id`,`gamespace_id`,`cluster_id`),
                KEY `cluster_id` (`cluster_id`),
                KEY `account_id_2` (`account_id`),
                KEY `cluster_id_2` (`cluster_id`),
                CONSTRAINT `leaderboard_cluster_accounts_ibfk_1`
                    FOREIGN KEY (`cluster_id`) REFERENCES `<table_name>` (`cluster_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

        """

        self.db = db
        self.table_name = table_name
        self.accounts_table_name = accounts_table_name

    @coroutine
    def delete_clusters(self, gamespace, key):
        """
        Deletes all cluster records around some key
        """
        with (yield self.db.acquire()) as db:
            yield self.delete_clusters(gamespace, key, db)

    @coroutine
    def delete_clusters_db(self, gamespace, key, db):
        yield db.execute(
            """
                DELETE FROM `{0}`
                WHERE `gamespace_id`=%s AND `cluster_data`=%s;
            """.format(self.accounts_table_name),
            gamespace, key)

        yield db.execute(
            """
                DELETE FROM `{0}`
                WHERE `gamespace_id`=%s AND `cluster_data`=%s;
            """.format(self.table_name),
            gamespace, key)

    @coroutine
    def list_clusters(self, gamespace, key, db=None):
        """
        Returns list of cluster ids for certain key
        """
        try:
            clusters = yield (db or self.db).query(
                """
                    SELECT `cluster_id`
                    FROM `{0}`
                    WHERE `gamespace_id`=%s AND `cluster_data`=%s;
                """.format(self.table_name),
                gamespace, key
            )
        except DatabaseError as e:
            raise ClusterError("Failed to list clusters: " + e.args[1])

        raise Return([cluster["cluster_id"] for cluster in clusters])

    @coroutine
    def leave_cluster(self, gamespace, account, key, add_vacant_place=True):
        """
        Leaves a cluster for a player
        :param gamespace: A gamespace
        :param account: Player's account
        :param key: A key of cluster to leave
        :param add_vacant_place: if true, a new vacant space for a new player would be created
        :return:
        """

        with (yield self.db.acquire(auto_commit=False)) as db:
            try:

                query = """
                    SELECT `{0}`.`cluster_id`, `{1}`.`cluster_size`
                    FROM `{0}`, `{1}`
                    WHERE `{0}`.`gamespace_id`=%s AND `{0}`.`account_id`=%s AND `{0}`.`cluster_data`=%s AND
                        `{1}`.`gamespace_id`=`{0}`.`gamespace_id` AND `{1}`.`cluster_id`=`{0}`.`cluster_id` AND
                        `{1}`.`cluster_data`=`{0}`.`cluster_data`
                    LIMIT 1
                    FOR UPDATE;
                """.format(self.accounts_table_name, self.table_name)

                # look for existent join
                cluster = yield db.get(query, gamespace, account, key)

                if not cluster:
                    return

                cluster_size = cluster["cluster_size"]
                cluster_id = cluster["cluster_id"]

                yield db.execute(
                    """
                        DELETE FROM `{0}`
                        WHERE `{0}`.`gamespace_id`=%s AND `{0}`.`cluster_id`=%s AND `{0}`.`account_id`=%s
                    """.format(self.accounts_table_name),
                    gamespace, cluster_id, account
                )

                if add_vacant_place:
                    updated_cluster_size = cluster_size + 1

                    yield db.execute(
                        """
                            UPDATE `{0}`
                            SET `cluster_size`=%s
                            WHERE `{0}`.`gamespace_id`=%s AND `{0}`.`cluster_id`=%s
                        """.format(self.table_name),
                        updated_cluster_size, gamespace, cluster_id
                    )

            except DatabaseError as e:
                raise ClusterError("Failed to leave account cluster: " + e.args[1])
            finally:
                yield db.commit()

    @coroutine
    def get_cluster(self, gamespace, account, key, auto_create=True, cluster_size=50):
        """
        Acquires a cluster for a player over a certain key

        :param gamespace: A gamespace
        :param account: Player's account
        :param key: A key to retrieve a cluster about
        :param auto_create: Spawn a new cluster if there's no a free one
        :param cluster_size: A cluster size for new cluster in case there's no a free one and auto_create is True
        :return: Id of acquired cluster for player for a key
        """

        try:
            # look for existent join
            cluster = yield self.db.get(
                """
                    SELECT `cluster_id`
                    FROM `{0}`
                    WHERE `gamespace_id`=%s AND `account_id`=%s AND `cluster_data`=%s
                    LIMIT 1;
                """.format(self.accounts_table_name),
                gamespace, account, key)
        except DatabaseError as e:
            raise ClusterError("Failed to get account cluster: " + e.args[1])

        if cluster:
            raise Return(cluster["cluster_id"])

        if auto_create:
            # if no account corresponding gamespace/key, then create a fresh new cluster
            raise Return((yield self.__new_cluster__(gamespace, account, key, cluster_size)))

        raise NoClusterError()

    @coroutine
    def __new_cluster__(self, gamespace, account, key, cluster_size):
        try:
            with (yield self.db.acquire(auto_commit=False)) as db:
                # find existing cluster with free rooms
                cluster = yield db.get(
                    """
                        SELECT `cluster_id`, `cluster_size`
                        FROM `{0}`
                        WHERE `gamespace_id`=%s AND `cluster_size` > 0 AND `cluster_data`=%s
                        LIMIT 1
                        FOR UPDATE;
                    """.format(self.table_name),
                    gamespace, key)

                if cluster:
                    # join this cluster, decrease cluster size

                    cluster_id = cluster["cluster_id"]
                    new_size = cluster["cluster_size"] - 1
                    yield db.execute(
                        """
                            UPDATE `{0}`
                            SET `cluster_size`=%s
                            WHERE `cluster_id`=%s;
                        """.format(self.table_name),
                        new_size, cluster_id
                    )

                    yield db.commit()
                    yield db.autocommit(True)

                    yield self.db.insert(
                        """
                            INSERT INTO `{0}`
                            (`gamespace_id`, `account_id`, `cluster_id`, `cluster_data`)
                            VALUES(%s, %s, %s, %s);
                        """.format(self.accounts_table_name),
                        gamespace, account, cluster_id, key)

                    raise Return(cluster_id)
                else:
                    yield db.autocommit(True)

                    # create new cluster, and join it

                    cluster_id = yield db.insert(
                        """
                            INSERT INTO `{0}`
                            (`gamespace_id`, `cluster_size`, `cluster_data`)
                            VALUES(%s, %s, %s);
                        """.format(self.table_name),
                        gamespace, cluster_size - 1, key)\

                    yield db.insert(
                        """
                            INSERT INTO `{0}`
                            (`gamespace_id`, `account_id`, `cluster_id`, `cluster_data`)
                            VALUES(%s, %s, %s, %s);
                        """.format(self.accounts_table_name),
                        gamespace, account, cluster_id, key)

                    raise Return(cluster_id)

        except DatabaseError as e:
            raise ClusterError("Failed to create a cluster: " + e.args[1])
