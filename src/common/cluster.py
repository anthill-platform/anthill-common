
from tornado.gen import coroutine, Return
from database import DatabaseError


class ClusterError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class Cluster(object):
    def __init__(self, db, table_name, accounts_table_name):
        self.db = db
        self.table_name = table_name
        self.accounts_table_name = accounts_table_name

    @coroutine
    def delete_clusters(self, gamespace, key, db=None):
        yield (db or self.db).execute(
            """
                DELETE FROM `{0}`
                WHERE `gamespace_id`=%s AND `cluster_data`=%s;
            """.format(self.accounts_table_name),
            gamespace, key)

        yield (db or self.db).execute(
            """
                DELETE FROM `{0}`
                WHERE `gamespace_id`=%s AND `cluster_data`=%s;
            """.format(self.table_name),
            gamespace, key)

    @coroutine
    def list_clusters(self, gamespace, key, db=None):
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
    def get_cluster(self, gamespace, account, key, cluster_size):
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

        # if no account corresponding gamespace/key, then create a fresh new cluster
        raise Return((yield self.__new_cluster__(gamespace, account, key, cluster_size)))

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
