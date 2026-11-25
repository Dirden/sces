# 在某一台机器上执行的动作。这个文件只用标准库，管理节点会把它通过 ssh 送到目标机上跑。
import base64
import json
import os
import shutil
import subprocess
import sys
import traceback
from contextlib import contextmanager

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None

INDEX_DATA = "/data/meta"
YML_PATH = "/etc/elasticsearch/elasticsearch.yml"
JVM_PATH = "/etc/elasticsearch/jvm.options"
SYSCTL_PATH = "/etc/sysctl.d/99-elasticsearch.conf"
MEMLOCK_PATH = "/etc/systemd/system/elasticsearch.service.d/override.conf"
FSTAB_PATH = "/etc/fstab"
BACKUP_ROOT = "/var/lib/sces/backup"

SPEC_KEYS = (
    "cluster_name",
    "node_name",
    "node_master",
    "network_host",
    "unicast_hosts",
    "minimum_master_nodes",
)


class Fail(Exception):
    pass


def join_root(root, path):
    if not root or root == "/":
        return path
    return os.path.join(root, path.lstrip("/"))


def run_cmd(cmd, timeout=60):
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise Fail("命令不存在: %s" % cmd[0]) from exc
    except subprocess.TimeoutExpired as exc:
        raise Fail("命令超时: %s" % " ".join(cmd)) from exc
    return proc.returncode, proc.stdout or "", proc.stderr or ""


@contextmanager
def locked(root, name):
    if fcntl is None:
        raise Fail("这台没有文件锁")
    path = join_root(root, "/var/lock/" + name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Fail("拿不到锁") from exc
        yield path
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def parse_yml(text):
    lines = text.splitlines()
    spans = {}
    i = 0
    while i < len(lines):
        raw = lines[i]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#") or raw[:1] in (" ", "\t", "-"):
            i += 1
            continue
        if ":" not in raw:
            i += 1
            continue
        key, rest = raw.split(":", 1)
        key = key.strip()
        if not key or " " in key:
            i += 1
            continue
        start = i
        rest = rest.strip()
        i += 1
        if rest == "":
            while i < len(lines):
                nxt = lines[i]
                if nxt.strip() == "" or nxt[:1] in (" ", "\t", "-"):
                    i += 1
                    continue
                break
        spans[key] = (start, i)
    return lines, spans


def render_updates(text, updates):
    """updates 是 (键, 渲染后的值) 列表。只替换这些键，其余行保持原样。"""
    lines, spans = parse_yml(text)
    changed = []
    existing = []
    fresh = []
    for key, rendered in updates:
        if key in spans:
            existing.append((spans[key][0], key, rendered))
        else:
            fresh.append((key, rendered))
    existing.sort(reverse=True)
    for _, key, rendered in existing:
        start, end = spans[key]
        new_line = "%s: %s" % (key, rendered)
        if end == start + 1 and lines[start] == new_line:
            continue
        lines[start:end] = [new_line]
        changed.append(key)
    for key, rendered in fresh:
        lines.append("%s: %s" % (key, rendered))
        changed.append(key)
    body = "\n".join(lines)
    if body and not body.endswith("\n"):
        body += "\n"
    if not body:
        body = "\n"
    return body, changed


def atomic_write(path, body):
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    tmp = path + ".sces-tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, body.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)


def yml_values(spec, creating):
    unknown = [k for k in spec if k not in SPEC_KEYS]
    if unknown:
        raise Fail("规格里有不能写的字段: %s" % ", ".join(unknown))
    if creating:
        missing = [k for k in SPEC_KEYS if k not in spec]
        if missing:
            raise Fail("新建 yml 缺少字段: %s" % ", ".join(missing))
    elif not any(k in spec for k in SPEC_KEYS):
        raise Fail("没有要改的字段")
    updates = []
    if "cluster_name" in spec:
        name = spec["cluster_name"]
        if not isinstance(name, str) or not name.strip() or "\n" in name:
            raise Fail("cluster_name 不合法")
        updates.append(("cluster.name", name.strip()))
    if "node_name" in spec:
        name = spec["node_name"]
        if not isinstance(name, str) or not name.strip() or "\n" in name:
            raise Fail("node_name 不合法")
        updates.append(("node.name", name.strip()))
    if "node_master" in spec:
        if not isinstance(spec["node_master"], bool):
            raise Fail("node_master 必须是 true 或 false")
        updates.append(("node.master", "true" if spec["node_master"] else "false"))
    if "network_host" in spec:
        host = spec["network_host"]
        if not isinstance(host, str) or not host.strip():
            raise Fail("network_host 不合法")
        updates.append(("network.host", host.strip()))
    if "unicast_hosts" in spec:
        hosts = spec["unicast_hosts"]
        if not isinstance(hosts, list) or not hosts or not all(isinstance(h, str) and h.strip() for h in hosts):
            raise Fail("unicast_hosts 必须是非空的地址列表")
        updates.append(("discovery.zen.ping.unicast.hosts", json.dumps([h.strip() for h in hosts], ensure_ascii=False)))
    if "minimum_master_nodes" in spec:
        if isinstance(spec["minimum_master_nodes"], bool):
            raise Fail("minimum_master_nodes 必须是整数")
        try:
            count = int(spec["minimum_master_nodes"])
        except (TypeError, ValueError) as exc:
            raise Fail("minimum_master_nodes 必须是整数") from exc
        if count < 1:
            raise Fail("minimum_master_nodes 必须是整数")
        updates.append(("discovery.zen.minimum_master_nodes", str(count)))
    if creating:
        updates.extend([
            ("node.data", "true"),
            ("http.port", "9200"),
            ("transport.tcp.port", "9300"),
            ("path.data", INDEX_DATA),
            ("path.logs", "/var/log/elasticsearch"),
            ("bootstrap.memory_lock", "true"),
        ])
    return updates


def yml_backup(root, placement):
    src = join_root(root, YML_PATH)
    if not os.path.isfile(src):
        raise Fail("elasticsearch.yml 不存在")
    stamp = _stamp()
    dest_dir = join_root(root, os.path.join(BACKUP_ROOT, placement, stamp))
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "elasticsearch.yml")
    shutil.copy2(src, dest)
    return {"path": dest, "files": [dest]}


def _stamp():
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d%H%M%S%f")


def yml_restore(root, placement, path):
    del placement
    if not path:
        raise Fail("缺少 --path")
    base = os.path.realpath(join_root(root, BACKUP_ROOT))
    real = os.path.realpath(path)
    if real != base and not real.startswith(base + os.sep):
        raise Fail("备份路径不在 /var/lib/sces/backup 下")
    if not os.path.isfile(real):
        raise Fail("备份文件不存在")
    dest = join_root(root, YML_PATH)
    with locked(root, "sces-elasticsearch.yml.lock"):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(real, "r", encoding="utf-8") as fh:
            atomic_write(dest, fh.read())
    return {"path": dest, "files": [dest]}


def yml_apply(root, spec):
    if not isinstance(spec, dict):
        raise Fail("规格必须是 JSON 对象")
    dest = join_root(root, YML_PATH)
    with locked(root, "sces-elasticsearch.yml.lock"):
        creating = not os.path.isfile(dest)
        updates = yml_values(spec, creating)
        current = ""
        if not creating:
            with open(dest, "r", encoding="utf-8") as fh:
                current = fh.read()
        body, changed = render_updates(current, updates)
        if changed:
            atomic_write(dest, body)
    return {"changed": changed, "files": [dest] if changed else []}


def jvm_apply(root, heap):
    if not isinstance(heap, str) or not _heap_ok(heap):
        raise Fail("heap 必须是像 8g 这样的值")
    heap = heap.strip()
    dest = join_root(root, JVM_PATH)
    if not os.path.isfile(dest):
        raise Fail("jvm.options 不存在，先安装 elasticsearch")
    with locked(root, "sces-jvm.options.lock"):
        with open(dest, "r", encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        found_ms = found_mx = False
        out = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("-Xms") and not stripped.startswith("#"):
                out.append("-Xms" + heap)
                found_ms = True
            elif stripped.startswith("-Xmx") and not stripped.startswith("#"):
                out.append("-Xmx" + heap)
                found_mx = True
            else:
                out.append(line)
        if not found_ms:
            out.append("-Xms" + heap)
        if not found_mx:
            out.append("-Xmx" + heap)
        atomic_write(dest, "\n".join(out) + "\n")
    return {"heap": heap, "files": [dest]}


def _heap_ok(heap):
    import re
    return re.match(r"^[1-9]\d*[gGmM]$", heap.strip() or "") is not None


def sysctl_apply(root, run=run_cmd):
    dest = join_root(root, SYSCTL_PATH)
    with locked(root, "sces-99-elasticsearch.conf.lock"):
        atomic_write(dest, "vm.max_map_count=262144\n")
        if root in ("", "/"):
            code, out, err = run(["sysctl", "-w", "vm.max_map_count=262144"], timeout=30)
            if code != 0:
                raise Fail((err or out or "sysctl 失败").strip())
    return {"files": [dest]}


def memlock_apply(root, run=run_cmd):
    dest = join_root(root, MEMLOCK_PATH)
    with locked(root, "sces-memlock.lock"):
        atomic_write(dest, "[Service]\nLimitMEMLOCK=infinity\n")
        if root in ("", "/"):
            code, out, err = run(["systemctl", "daemon-reload"], timeout=60)
            if code != 0:
                raise Fail((err or out or "daemon-reload 失败").strip())
    return {"files": [dest]}


def select_free_disks(tree):
    devices = tree.get("blockdevices") if isinstance(tree, dict) else None
    if not isinstance(devices, list):
        raise Fail("lsblk 没有返回磁盘列表")
    free = []
    for dev in devices:
        if dev.get("type") != "disk":
            continue
        if _disk_busy(dev):
            continue
        free.append({
            "name": dev.get("name"),
            "size": dev.get("size"),
            "type": "disk",
        })
    return free


def _mounts(dev):
    found = []
    mp = dev.get("mountpoint") or dev.get("mountpoints") or ""
    if isinstance(mp, list):
        found.extend([m for m in mp if m])
    elif mp:
        found.append(mp)
    for child in dev.get("children") or []:
        found.extend(_mounts(child))
    return found


def _disk_busy(dev):
    return bool(_mounts(dev))


def _load_lsblk(run):
    code, out, err = run(["lsblk", "-J", "-o", "NAME,SIZE,TYPE,MOUNTPOINT"], timeout=30)
    if code != 0:
        raise Fail((err or out or "lsblk 失败").strip())
    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise Fail("lsblk 没有返回 JSON") from exc


def disk_get(run=run_cmd):
    return {"disks": select_free_disks(_load_lsblk(run))}


def _find_disk(tree, name):
    for dev in tree.get("blockdevices") or []:
        if dev.get("name") == name and dev.get("type") == "disk":
            return dev
    return None


def disk_mount(root, disk, run=run_cmd):
    if not disk or "/" in disk or disk.startswith("."):
        raise Fail("磁盘名不合法")
    tree = _load_lsblk(run)
    dev = _find_disk(tree, disk)
    if dev is None:
        raise Fail("不是空闲盘")
    mounts = _mounts(dev)
    if mounts:
        raise Fail("不是空闲盘")
    if dev.get("children"):
        raise Fail("这块盘已有分区，不格式化")
    device = "/dev/" + disk
    code, fstype, err = run(["blkid", "-o", "value", "-s", "TYPE", device], timeout=30)
    has_fs = code == 0 and bool(fstype.strip())
    if not has_fs:
        code, out, err = run(["mkfs.xfs", "-f", "-K", device], timeout=600)
        if code != 0:
            raise Fail((err or out or "mkfs.xfs 失败").strip())
    code, uuid, err = run(["blkid", "-o", "value", "-s", "UUID", device], timeout=30)
    uuid = uuid.strip()
    if code != 0 or not uuid:
        raise Fail((err or "读不到 UUID").strip())
    meta = join_root(root, INDEX_DATA)
    os.makedirs(meta, exist_ok=True)
    if root in ("", "/"):
        code, out, err = run(["mount", device, INDEX_DATA], timeout=60)
        if code != 0:
            raise Fail((err or out or "mount 失败").strip())
    fstab = join_root(root, FSTAB_PATH)
    line = "UUID=%s /data/meta xfs defaults,noatime 0 0" % uuid
    with locked(root, "sces-fstab.lock"):
        current = ""
        if os.path.isfile(fstab):
            with open(fstab, "r", encoding="utf-8") as fh:
                current = fh.read()
        if not fstab_has_meta(current):
            if current and not current.endswith("\n"):
                current += "\n"
            atomic_write(fstab, current + line + "\n")
    owner = _chown_meta(meta)
    os.chmod(meta, 0o2750)
    return {"uuid": uuid, "source": device, "owner": owner, "files": [fstab]}


def fstab_has_meta(text):
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split()
        if len(parts) >= 2 and parts[1] == INDEX_DATA:
            return True
    return False


def fstab_without_meta(text):
    kept = []
    for line in text.splitlines():
        stripped = line.strip()
        parts = stripped.split()
        if stripped and not stripped.startswith("#") and len(parts) >= 2 and parts[1] == INDEX_DATA:
            continue
        kept.append(line)
    body = "\n".join(kept)
    if body and not body.endswith("\n"):
        body += "\n"
    return body


def _chown_meta(meta):
    import pwd
    import grp
    try:
        uid = pwd.getpwnam("elasticsearch").pw_uid
        gid = grp.getgrnam("elasticsearch").gr_gid
    except KeyError:
        return None
    os.chown(meta, uid, gid)
    return "elasticsearch"


def _service_state(run):
    code, out, err = run(["systemctl", "is-active", "elasticsearch"], timeout=30)
    state = (out or "").strip()
    if state in ("active", "inactive", "failed", "activating", "deactivating", "unknown"):
        return state
    if code != 0:
        raise Fail((err or out or "读不到 elasticsearch 状态").strip())
    return state or "unknown"


def disk_clean(root, run=run_cmd):
    if root in ("", "/") and _service_state(run) == "active":
        raise Fail("elasticsearch 还在运行，不动盘")
    meta = join_root(root, INDEX_DATA)
    if os.path.isdir(meta) and not os.path.islink(meta):
        for name in os.listdir(meta):
            path = os.path.join(meta, name)
            if os.path.isdir(path) and not os.path.islink(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
    if root in ("", "/"):
        code, out, err = run(["findmnt", "-n", INDEX_DATA], timeout=30)
        if code == 0:
            code, out, err = run(["umount", INDEX_DATA], timeout=60)
            if code != 0:
                raise Fail((err or out or "umount 失败").strip())
    fstab = join_root(root, FSTAB_PATH)
    with locked(root, "sces-fstab.lock"):
        if os.path.isfile(fstab):
            with open(fstab, "r", encoding="utf-8") as fh:
                current = fh.read()
            atomic_write(fstab, fstab_without_meta(current))
    return {"files": [fstab]}


def _installed_version(run):
    code, out, err = run(["rpm", "-q", "--queryformat", "%{VERSION}", "elasticsearch"], timeout=30)
    if code != 0:
        return None
    return out.strip() or None


def package_install(root, run=run_cmd):
    with locked(root, "sces-package.lock"):
        current = _installed_version(run) if root in ("", "/") else None
        if current == "6.8.23":
            _chown_if_present(root)
            return {"installed": "6.8.23", "changed": False, "files": []}
        if current:
            raise Fail("已安装的版本不是 6.8.23")
        if root not in ("", "/"):
            raise Fail("测试根目录不能安装软件包")
        installer = "dnf" if shutil.which("dnf") else "yum"
        code, out, err = run([installer, "-y", "install", "elasticsearch-6.8.23"], timeout=600)
        if code != 0:
            raise Fail((err or out or "安装失败").strip())
        got = _installed_version(run)
        if got != "6.8.23":
            raise Fail("安装后的版本不是 6.8.23")
        _stop_if_active(run, root)
        _chown_if_present(root)
    return {"installed": "6.8.23", "changed": True, "files": []}


def _stop_if_active(run, root):
    if root not in ("", "/"):
        return
    if _service_state(run) == "active":
        code, out, err = run(["systemctl", "stop", "elasticsearch"], timeout=180)
        if code != 0:
            raise Fail((err or out or "安装后停止进程失败").strip())


def _chown_if_present(root):
    meta = join_root(root, INDEX_DATA)
    if os.path.isdir(meta):
        _chown_meta(meta)


def package_remove(root, run=run_cmd):
    if root not in ("", "/"):
        raise Fail("测试根目录不能卸载软件包")
    with locked(root, "sces-package.lock"):
        if _service_state(run) == "active":
            raise Fail("elasticsearch 还在运行，不卸载")
        if _installed_version(run) is None:
            return {"removed": False, "files": []}
        code, out, err = run(["rpm", "-e", "elasticsearch"], timeout=600)
        if code != 0:
            raise Fail((err or out or "卸载失败").strip())
    return {"removed": True, "files": []}


def service_op(op, run=run_cmd):
    if op not in ("start", "stop", "restart", "enable", "disable"):
        raise Fail("不支持的动作")
    with locked("/", "sces-elasticsearch.service.lock"):
        code, out, err = run(["systemctl", op, "elasticsearch"], timeout=180)
        if code != 0:
            raise Fail((err or out or "systemctl %s 失败" % op).strip())
        state = _service_state(run)
    return {"active": state, "files": []}


def _port_listen(run, port):
    code, out, err = run(["ss", "-lnt"], timeout=30)
    if code != 0:
        raise Fail((err or out or "ss 失败").strip())
    needle = ":%s" % port
    for line in out.splitlines():
        if needle in line.split():
            return True
        # ss 把本地地址和端口放在一列，形如 0.0.0.0:7485 或 *:7485
        if any(col.endswith(needle) for col in line.split()):
            return True
    return False


def _unit_from_ss(run):
    code, out, err = run(["ss", "-lntp"], timeout=30)
    if code != 0:
        return None
    import re
    match = re.search(r":7485\b.*pid=(\d+)", out)
    if not match:
        return None
    code, status, err = run(["systemctl", "status", match.group(1), "--no-pager"], timeout=30)
    text = status or err or ""
    found = re.search(r"((?:ceph-radosgw@|radosgw)[A-Za-z0-9@._:-]*\.service)", text)
    if found:
        return found.group(1)
    return None


def _unit_from_files(run):
    code, out, err = run(["systemctl", "list-unit-files", "--no-legend", "ceph-radosgw@*"], timeout=30)
    if code != 0:
        return None
    units = []
    for line in out.splitlines():
        name = line.split()[0] if line.split() else ""
        if not name.endswith(".service"):
            continue
        code, body, err = run(["systemctl", "cat", name], timeout=30)
        text = body or ""
        if ":7485" in text or "7485" in text:
            units.append(name)
    if len(units) == 1:
        return units[0]
    if len(units) > 1:
        raise Fail("这台有多个 7485 网关单元")
    return None


def gateway_unit(run):
    unit = _unit_from_ss(run)
    if unit:
        return unit
    return _unit_from_files(run)


def gateway_op(op, run=run_cmd):
    if op not in ("start", "stop", "restart"):
        raise Fail("不支持的动作")
    with locked("/", "sces-gateway.lock"):
        unit = gateway_unit(run)
        if not unit:
            raise Fail("这台没有检索网关单元")
        code, out, err = run(["systemctl", op, unit], timeout=180)
        if code != 0:
            raise Fail((err or out or "systemctl %s 失败" % op).strip())
        listen = _port_listen(run, 7485)
    return {"unit": unit, "listen": listen, "files": []}


def node_get(run=run_cmd):
    state = _service_state(run)
    code, out, err = run(["findmnt", "-n", "-o", "SOURCE", INDEX_DATA], timeout=30)
    source = out.strip() if code == 0 and out.strip() else None
    listen = _port_listen(run, 7485)
    unit = None
    try:
        unit = gateway_unit(run)
    except Fail:
        unit = None
    active = False
    if unit:
        code, out, err = run(["systemctl", "is-active", unit], timeout=30)
        active = out.strip() == "active"
    if listen:
        active = True
    return {
        "elasticsearch": state,
        "data_mount": {"path": INDEX_DATA, "source": source},
        "gateway_7485": {"listen": listen, "active": active},
    }


def dispatch(req):
    action = req.get("action")
    root = req.get("root") or "/"
    placement = req.get("placement") or ""
    if action == "yml_backup":
        return yml_backup(root, placement)
    if action == "yml_restore":
        return yml_restore(root, placement, req.get("path"))
    if action == "yml_apply":
        return yml_apply(root, req.get("spec"))
    if action == "jvm_apply":
        return jvm_apply(root, req.get("heap"))
    if action == "sysctl_apply":
        return sysctl_apply(root)
    if action == "memlock_apply":
        return memlock_apply(root)
    if action == "disk_get":
        data = disk_get()
        data["placement"] = placement
        return data
    if action == "disk_mount":
        data = disk_mount(root, req.get("disk"))
        data["placement"] = placement
        return data
    if action == "disk_clean":
        return disk_clean(root)
    if action == "package_install":
        return package_install(root)
    if action == "package_remove":
        return package_remove(root)
    if action == "service":
        data = service_op(req.get("op"))
        data["placement"] = placement
        return data
    if action == "gateway":
        data = gateway_op(req.get("op"))
        data["placement"] = placement
        return data
    if action == "node_get":
        data = node_get()
        data["placement"] = placement
        return data
    raise Fail("不支持的动作")


def dispatch_main():
    try:
        raw = base64.b64decode(sys.argv[1])
        req = json.loads(raw)
        out = {"ok": True, "data": dispatch(req)}
    except Fail as exc:
        out = {"ok": False, "error": str(exc)}
    except Exception as exc:
        traceback.print_exc(file=sys.stderr)
        out = {"ok": False, "error": str(exc)}
    sys.stdout.write(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    dispatch_main()
