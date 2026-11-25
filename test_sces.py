# 不连集群、不改本机 /etc 的检查。
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from core.cluster_ops import apply_zone_spec, merge_nodes, parse_recovery, parse_shards, pick_setting
from core.node_worker import Fail, disk_clean, disk_mount, dispatch, fstab_without_meta, jvm_apply, select_free_disks

SCES = os.path.join(ROOT, "sces")


FULL_SPEC = {
    "cluster_name": "es-meta",
    "node_name": "10.0.0.8",
    "node_master": True,
    "network_host": "10.0.0.8",
    "unicast_hosts": ["10.0.0.8", "10.0.0.9"],
    "minimum_master_nodes": 2,
}


def run_cli(args):
    return subprocess.run([sys.executable, SCES, *args], capture_output=True, text=True)


class YmlTest(unittest.TestCase):
    def test_create_then_partial_update(self):
        root = tempfile.mkdtemp()
        created = dispatch({"action": "yml_apply", "root": root, "spec": FULL_SPEC})
        self.assertIn("discovery.zen.minimum_master_nodes", created["changed"])
        path = os.path.join(root, "etc/elasticsearch/elasticsearch.yml")
        text = open(path, encoding="utf-8").read()
        self.assertIn("http.port: 9200", text)
        self.assertIn('discovery.zen.ping.unicast.hosts: ["10.0.0.8", "10.0.0.9"]', text)
        self.assertIn("node.master: true", text)

        # 已有文件里发现列表是多行的，只改选主人数时，其它行要留着。
        open(path, "w", encoding="utf-8").write(textwrap.dedent("""\
            # keep
            cluster.name: es-meta
            discovery.zen.ping.unicast.hosts:
              - 10.0.0.8
              - 10.0.0.9
            http.port: 9200
        """))
        updated = dispatch({
            "action": "yml_apply",
            "root": root,
            "spec": {"minimum_master_nodes": 3},
        })
        body = open(path, encoding="utf-8").read()
        self.assertEqual(updated["changed"], ["discovery.zen.minimum_master_nodes"])
        self.assertIn("# keep", body)
        self.assertIn("http.port: 9200", body)
        self.assertIn("discovery.zen.minimum_master_nodes: 3", body)
        self.assertIn("- 10.0.0.8", body)
        replaced = dispatch({
            "action": "yml_apply",
            "root": root,
            "spec": {"unicast_hosts": ["10.0.0.7"]},
        })
        body = open(path, encoding="utf-8").read()
        self.assertIn("discovery.zen.ping.unicast.hosts", replaced["changed"])
        self.assertIn('discovery.zen.ping.unicast.hosts: ["10.0.0.7"]', body)
        self.assertNotIn("- 10.0.0.8", body)
        self.assertIn("http.port: 9200", body)

    def test_unknown_field_does_not_write(self):
        root = tempfile.mkdtemp()
        with self.assertRaises(Fail):
            dispatch({"action": "yml_apply", "root": root, "spec": {"cluster_name": "a", "port": 1}})
        self.assertFalse(os.path.exists(os.path.join(root, "etc/elasticsearch/elasticsearch.yml")))

    def test_backup_and_restore_roundtrip(self):
        root = tempfile.mkdtemp()
        dispatch({"action": "yml_apply", "root": root, "placement": "10.0.0.8", "spec": FULL_SPEC})
        saved = dispatch({"action": "yml_backup", "root": root, "placement": "10.0.0.8"})
        path = os.path.join(root, "etc/elasticsearch/elasticsearch.yml")
        open(path, "w", encoding="utf-8").write("cluster.name: changed\n")
        dispatch({"action": "yml_restore", "root": root, "placement": "10.0.0.8", "path": saved["path"]})
        self.assertIn("es-meta", open(path, encoding="utf-8").read())
        with self.assertRaises(Fail):
            dispatch({"action": "yml_restore", "root": root, "path": "/etc/passwd"})

    def test_lock_refuses_second_writer(self):
        root = tempfile.mkdtemp()
        dispatch({"action": "yml_apply", "root": root, "spec": FULL_SPEC})
        holder = subprocess.Popen([sys.executable, "-c", textwrap.dedent("""\
            import sys, time
            sys.path.insert(0, %r)
            from core.node_worker import locked
            with locked(%r, "sces-elasticsearch.yml.lock"):
                time.sleep(2)
        """ % (ROOT, root))])
        time.sleep(0.4)
        try:
            with self.assertRaises(Fail) as caught:
                dispatch({"action": "yml_apply", "root": root, "spec": {"minimum_master_nodes": 4}})
            self.assertIn("拿不到锁", str(caught.exception))
        finally:
            holder.wait(timeout=5)


class FileTest(unittest.TestCase):
    def test_jvm_replaces_heap(self):
        root = tempfile.mkdtemp()
        path = os.path.join(root, "etc/elasticsearch/jvm.options")
        os.makedirs(os.path.dirname(path))
        open(path, "w", encoding="utf-8").write("# comment\n-Xms1g\n-Xmx1g\n")
        jvm_apply(root, "8g")
        body = open(path, encoding="utf-8").read()
        self.assertIn("# comment", body)
        self.assertIn("-Xms8g", body)
        self.assertIn("-Xmx8g", body)

    def test_fstab_and_clean(self):
        root = tempfile.mkdtemp()
        meta = os.path.join(root, "data/meta")
        os.makedirs(meta)
        open(os.path.join(meta, "a"), "w", encoding="utf-8").write("x")
        fstab = os.path.join(root, "etc/fstab")
        os.makedirs(os.path.dirname(fstab))
        open(fstab, "w", encoding="utf-8").write("UUID=abc / / xfs defaults 0 0\nUUID=def /data/meta xfs defaults,noatime 0 0\n")
        disk_clean(root, run=lambda *a, **k: (0, "inactive\n", ""))
        self.assertEqual(os.listdir(meta), [])
        left = open(fstab, encoding="utf-8").read()
        self.assertIn("UUID=abc / ", left)
        self.assertNotIn("/data/meta", left)
        self.assertEqual(fstab_without_meta("UUID=a /data/meta xfs defaults 0 0\n"), "")

    def test_mount_formats_only_empty_disk(self):
        root = tempfile.mkdtemp()
        calls = []

        def run(cmd, timeout=60):
            calls.append(cmd[0])
            if cmd[0] == "lsblk":
                tree = {"blockdevices": [
                    {"name": "sdc", "size": "1T", "type": "disk", "mountpoint": None, "children": []},
                ]}
                return 0, json.dumps(tree), ""
            if cmd[0] == "blkid" and "TYPE" in cmd:
                return 2, "", ""
            if cmd[0] == "blkid" and "UUID" in cmd:
                return 0, "UUID-1\n", ""
            return 0, "", ""

        data = disk_mount(root, "sdc", run=run)
        self.assertEqual(data["uuid"], "UUID-1")
        self.assertIn("mkfs.xfs", calls)
        fstab = open(os.path.join(root, "etc/fstab"), encoding="utf-8").read()
        self.assertIn("UUID=UUID-1 /data/meta xfs defaults,noatime 0 0", fstab)

        def run_has_fs(cmd, timeout=60):
            calls.append("again-" + cmd[0])
            if cmd[0] == "lsblk":
                tree = {"blockdevices": [
                    {"name": "sdd", "size": "1T", "type": "disk", "mountpoint": None,
                     "children": [{"name": "sdd1", "mountpoint": None}]},
                ]}
                return 0, json.dumps(tree), ""
            return 0, "", ""

        with self.assertRaises(Fail):
            disk_mount(root, "sdd", run=run_has_fs)
        self.assertNotIn("again-mkfs.xfs", calls)

    def test_free_disks_skip_root_and_mounted(self):
        tree = {"blockdevices": [
            {"name": "sda", "size": "100G", "type": "disk", "mountpoint": None,
             "children": [{"name": "sda1", "mountpoint": "/"}]},
            {"name": "sdb", "size": "1.8T", "type": "disk", "mountpoint": None, "children": []},
            {"name": "sdc", "size": "1T", "type": "disk", "mountpoint": "/data", "children": []},
            {"name": "sr0", "size": "1G", "type": "rom", "mountpoint": None},
        ]}
        self.assertEqual(select_free_disks(tree), [{"name": "sdb", "size": "1.8T", "type": "disk"}])


class ParseTest(unittest.TestCase):
    def test_nodes_and_shards(self):
        nodes = merge_nodes(
            "node-1 10.0.0.8 mdi 2gb *\nnode-2 10.0.0.9 di 2gb -\n",
            "node-1 10.0.0.8 40\nnode-2 10.0.0.9 71\n",
        )
        self.assertTrue(nodes[0]["master"])
        self.assertFalse(nodes[1]["master"])
        self.assertEqual(nodes[1]["disk_percent"], 71)
        shards = parse_shards("0 p STARTED node-1\n1 r RELOCATING node-1 -> 10.0.0.9 abc\n2 p UNASSIGNED\n")
        self.assertEqual(shards[1]["node"], "node-1 -> 10.0.0.9 abc")
        self.assertIsNone(shards[2]["node"])
        recovery = parse_recovery("sovecloud-object-meta 1 index node-1 node-2\n")
        self.assertEqual(recovery[0]["target_node"], "node-2")

    def test_setting_prefers_persistent(self):
        doc = {
            "defaults": {"discovery": {"zen": {"minimum_master_nodes": "1"}}},
            "persistent": {"discovery": {"zen": {"minimum_master_nodes": "3"}}},
        }
        self.assertEqual(pick_setting(doc, "minimum_master_nodes"), "3")

    def test_zone_set_only_touches_address(self):
        zone = {
            "name": "es",
            "endpoints": ["http://10.0.0.1:7485"],
            "tier_config": {
                "endpoint": "http://10.0.0.1:9200",
                "num_shards": "5",
                "explicit_custom_meta": "true",
                "index_buckets_list": "/",
            },
        }
        apply_zone_spec(zone, {
            "endpoint": "http://10.0.0.9:9200",
            "endpoints": ["http://10.0.0.8:7485", "http://10.0.0.9:7485"],
        })
        self.assertEqual(zone["tier_config"]["endpoint"], "http://10.0.0.9:9200")
        self.assertEqual(zone["tier_config"]["num_shards"], "5")
        self.assertEqual(zone["tier_config"]["index_buckets_list"], "/")
        self.assertEqual(zone["tier_config"]["explicit_custom_meta"], "false")
        self.assertEqual(len(zone["endpoints"]), 2)


class CliTest(unittest.TestCase):
    def test_missing_command_and_caller_are_json(self):
        missing = run_cli([])
        self.assertEqual(missing.returncode, 1)
        self.assertFalse(json.loads(missing.stdout)["ok"])
        no_caller = run_cli(["service", "--op", "stop", "--placement", "10.1.1.1"])
        self.assertEqual(no_caller.returncode, 1)
        self.assertEqual(json.loads(no_caller.stdout)["error"], "缺少 --caller")
        self.assertEqual(no_caller.stderr, "")


if __name__ == "__main__":
    unittest.main()
