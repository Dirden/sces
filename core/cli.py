# 命令行。一次调用只做一件事，结果只打到标准输出。
import argparse
import ipaddress
import json
import os
import subprocess
import sys
from datetime import datetime

from core.cluster_ops import (
    Fail,
    clear_readonly,
    cluster_get,
    exclude_clear,
    exclude_set,
    node_list,
    shard_get,
    watermark_apply,
    zone_get,
    zone_set,
)
from core.node_worker import dispatch

AUDIT_PATH = "/var/log/sces/audit.log"
WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "node_worker.py")

WRITE_OPS = {
    ("yml", "backup"), ("yml", "restore"), ("yml", "apply"),
    ("jvm", "apply"),
    ("sysctl", "apply"),
    ("memlock", "apply"),
    ("disk", "mount"), ("disk", "clean"),
    ("package", "install"), ("package", "remove"),
    ("service", "start"), ("service", "stop"), ("service", "restart"),
    ("service", "enable"), ("service", "disable"),
    ("exclude", "set"), ("exclude", "clear"),
    ("watermark", "apply"),
    ("index", "clear-readonly"),
    ("zone", "set"),
    ("gateway", "start"), ("gateway", "stop"), ("gateway", "restart"),
}


class Parser(argparse.ArgumentParser):
    def error(self, message):
        emit(False, error=message)
        sys.exit(1)


def emit(ok, data=None, error=None):
    if ok:
        body = {"ok": True, "data": data if data is not None else {}}
    else:
        body = {"ok": False, "error": error or "失败"}
    sys.stdout.write(json.dumps(body, ensure_ascii=False, indent=2) + "\n")


def audit(caller, obj, op, placement, files, ok, error=None):
    os.makedirs(os.path.dirname(AUDIT_PATH), exist_ok=True)
    row = {
        "time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "caller": caller,
        "object": obj,
        "op": op,
        "placement": placement,
        "files": files or [],
        "ok": ok,
    }
    if error:
        row["error"] = error
    with open(AUDIT_PATH, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def check_ip(value, label):
    try:
        ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError as exc:
        raise Fail("%s 不是 IP" % label) from exc


def local_ips():
    found = {"127.0.0.1"}
    try:
        proc = subprocess.run(["ip", "-4", "-o", "addr", "show"], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return found
    for line in proc.stdout.splitlines():
        parts = line.split()
        if "inet" in parts:
            addr = parts[parts.index("inet") + 1].split("/")[0]
            found.add(addr)
    return found


def is_local(ip):
    return ip in local_ips()


def load_spec(path):
    if not path:
        raise Fail("缺少 --path")
    if not os.path.isfile(path):
        raise Fail("规格文件不存在")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            spec = json.load(fh)
    except json.JSONDecodeError as exc:
        raise Fail("规格不是 JSON") from exc
    if not isinstance(spec, dict):
        raise Fail("规格必须是 JSON 对象")
    return spec


def on_node(ip, req, timeout):
    check_ip(ip, "--placement")
    req = dict(req)
    req["placement"] = ip
    req["root"] = "/"
    if is_local(ip):
        return dispatch(req)
    import base64
    with open(WORKER, "r", encoding="utf-8") as fh:
        script = fh.read()
    arg = base64.b64encode(json.dumps(req).encode("utf-8")).decode("ascii")
    cmd = [
        "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
        "-o", "StrictHostKeyChecking=accept-new",
        "root@" + ip, "python3", "-", arg,
    ]
    try:
        proc = subprocess.run(cmd, input=script, text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise Fail("这台连不上") from exc
    if proc.returncode != 0 or not proc.stdout.strip():
        err = (proc.stderr or proc.stdout or "这台连不上").strip()
        raise Fail(err.splitlines()[-1] if err else "这台连不上")
    try:
        msg = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise Fail("这台没有返回 JSON") from exc
    if not msg.get("ok"):
        raise Fail(msg.get("error") or "失败")
    return msg.get("data") or {}


def perform(args):
    obj = args.command
    op = args.op
    if obj == "zone" and op == "get":
        return zone_get()
    if obj == "zone" and op == "set":
        return zone_set(load_spec(args.path))
    if obj == "cluster" and op == "get":
        return cluster_get()
    if obj == "node" and op == "get" and not args.placement:
        return node_list()
    if obj == "node" and op == "get":
        return on_node(args.placement, {"action": "node_get"}, 90)
    if obj == "disk" and op == "get":
        return on_node(args.placement, {"action": "disk_get"}, 90)
    if obj == "disk" and op == "mount":
        return on_node(args.placement, {"action": "disk_mount", "disk": args.disk}, 660)
    if obj == "disk" and op == "clean":
        return on_node(args.placement, {"action": "disk_clean"}, 120)
    if obj == "shard" and op == "get":
        return shard_get()
    if obj == "yml" and op == "backup":
        return on_node(args.placement, {"action": "yml_backup"}, 90)
    if obj == "yml" and op == "restore":
        return on_node(args.placement, {"action": "yml_restore", "path": args.path}, 90)
    if obj == "yml" and op == "apply":
        return on_node(args.placement, {"action": "yml_apply", "spec": load_spec(args.path)}, 90)
    if obj == "jvm" and op == "apply":
        return on_node(args.placement, {"action": "jvm_apply", "heap": args.heap}, 90)
    if obj == "sysctl" and op == "apply":
        return on_node(args.placement, {"action": "sysctl_apply"}, 90)
    if obj == "memlock" and op == "apply":
        return on_node(args.placement, {"action": "memlock_apply"}, 90)
    if obj == "package" and op == "install":
        return on_node(args.placement, {"action": "package_install"}, 660)
    if obj == "package" and op == "remove":
        return on_node(args.placement, {"action": "package_remove"}, 660)
    if obj == "service" and op in ("start", "stop", "restart", "enable", "disable"):
        return on_node(args.placement, {"action": "service", "op": op}, 200)
    if obj == "exclude" and op == "set":
        check_ip(args.ip, "--ip")
        return exclude_set(args.ip)
    if obj == "exclude" and op == "clear":
        return exclude_clear()
    if obj == "watermark" and op == "apply":
        return watermark_apply()
    if obj == "index" and op == "clear-readonly":
        return clear_readonly()
    if obj == "gateway" and op in ("start", "stop", "restart"):
        return on_node(args.placement, {"action": "gateway", "op": op}, 200)
    raise Fail("不支持的动作")


def build_parser():
    parser = Parser(prog="sces", description="元数据检索命令")
    sub = parser.add_subparsers(dest="command")

    def add(name, help_text, ops):
        item = sub.add_parser(name, help=help_text)
        item.add_argument("--op", required=True, choices=ops)
        item.add_argument("--placement", help="节点 IP")
        item.add_argument("--path", help="规格文件或备份路径")
        item.add_argument("--caller", help="调用方")
        item.add_argument("--heap", help="堆内存，例如 8g")
        item.add_argument("--disk", help="磁盘名，例如 sdc")
        item.add_argument("--ip", help="排除的节点 IP")
        return item

    add("zone", "检索域", ["get", "set"])
    add("cluster", "集群", ["get"])
    add("node", "节点", ["get"])
    add("disk", "磁盘", ["get", "mount", "clean"])
    add("shard", "分片", ["get"])
    add("yml", "elasticsearch.yml", ["backup", "apply", "restore"])
    add("jvm", "堆内存", ["apply"])
    add("sysctl", "vm.max_map_count", ["apply"])
    add("memlock", "锁定内存", ["apply"])
    add("package", "安装包", ["install", "remove"])
    add("service", "检索进程", ["start", "stop", "restart", "enable", "disable"])
    add("exclude", "排除规则", ["set", "clear"])
    add("watermark", "水位", ["apply"])
    add("index", "索引", ["clear-readonly"])
    add("gateway", "检索网关", ["start", "stop", "restart"])
    return parser


def need_placement(args):
    if args.command in ("yml", "jvm", "sysctl", "memlock", "package", "service", "gateway"):
        return True
    if args.command == "disk":
        return True
    if args.command == "node" and args.op == "get":
        return False
    return False


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        emit(False, error="缺少命令")
        return 1
    writing = (args.command, args.op) in WRITE_OPS
    try:
        if writing and not (args.caller or "").strip():
            raise Fail("缺少 --caller")
        if need_placement(args) and not args.placement:
            raise Fail("缺少 --placement")
        if args.command == "disk" and args.op == "mount" and not args.disk:
            raise Fail("缺少 --disk")
        if args.command == "jvm" and not args.heap:
            raise Fail("缺少 --heap")
        if args.command == "exclude" and args.op == "set" and not args.ip:
            raise Fail("缺少 --ip")
        data = perform(args)
        if writing:
            audit(args.caller.strip(), args.command, args.op, args.placement, (data or {}).get("files"), True)
        emit(True, data=data)
        return 0
    except Fail as exc:
        if writing and (args.caller or "").strip():
            try:
                audit(args.caller.strip(), args.command, args.op, args.placement, [], False, str(exc))
            except OSError:
                pass
        emit(False, error=str(exc))
        return 1
    except Exception as exc:
        emit(False, error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
