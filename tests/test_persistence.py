"""
Unit tests for AOF and Snapshot persistence.
"""
import os
import shutil
import tempfile
import time
import unittest

from pyredis.persistence import AOFManager, AOFSyncPolicy, SnapshotManager
from pyredis.storage import Database, RedisDataType, RedisObject


class TestPersistence(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.aof_path = os.path.join(self.test_dir, "test.aof")
        self.rdb_path = os.path.join(self.test_dir, "test.rdb")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_aof_append_and_replay(self):
        aof = AOFManager(self.aof_path, sync_policy=AOFSyncPolicy.ALWAYS)
        aof.append([b"SET", b"name", b"RedisClone"])
        aof.append([b"SET", b"framework", b"Python"])
        aof.close()

        # Replay into fresh DB
        db = Database()
        executed_commands = []

        def executor(cmd):
            executed_commands.append(cmd)
            if cmd[0].upper() == b"SET":
                db.set(cmd[1], RedisObject(RedisDataType.STRING, cmd[2]))

        replayed_aof = AOFManager(self.aof_path, enabled=True)
        count = replayed_aof.replay(executor)
        replayed_aof.close()

        self.assertEqual(count, 2)
        self.assertTrue(db.exists(b"name"))
        self.assertEqual(db.get(b"name").value, b"RedisClone")
        self.assertEqual(db.get(b"framework").value, b"Python")

    def test_aof_rewrite_compaction(self):
        aof = AOFManager(self.aof_path, sync_policy=AOFSyncPolicy.ALWAYS)
        # Write 50 SETs to the same key
        for i in range(50):
            aof.append([b"SET", b"counter", str(i).encode("ascii")])
        aof.flush(force=True)

        initial_size = os.path.getsize(self.aof_path)

        # Database currently has counter = 49
        db = Database()
        db.set(b"counter", RedisObject(RedisDataType.STRING, b"49"))

        # Trigger rewrite
        aof.rewrite([db])
        aof.close()

        compacted_size = os.path.getsize(self.aof_path)
        self.assertLess(compacted_size, initial_size)

        # Replaying rewritten file should yield 1 command
        replayed_cmds = []
        fresh_aof = AOFManager(self.aof_path)
        fresh_aof.replay(lambda cmd: replayed_cmds.append(cmd))
        fresh_aof.close()

        self.assertEqual(len(replayed_cmds), 1)
        self.assertEqual(replayed_cmds[0], [b"SET", b"counter", b"49"])

    def test_snapshot_save_and_load(self):
        db = Database(db_id=0)
        db.set(b"snap_key", RedisObject(RedisDataType.STRING, b"snap_val"))

        snap = SnapshotManager(self.rdb_path)
        snap.save([db])

        # Restore into empty DB
        fresh_db = Database(db_id=0)
        loaded = snap.load([fresh_db])
        self.assertTrue(loaded)
        self.assertTrue(fresh_db.exists(b"snap_key"))
        self.assertEqual(fresh_db.get(b"snap_key").value, b"snap_val")


if __name__ == "__main__":
    unittest.main()
