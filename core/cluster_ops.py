# 在管理节点上读检索域、打 9200、写集群设置。这些动作不送到某一台业务机。
import json
import os
import subprocess
import tempfile

from core.node_worker import Fail, atomic_write, locked

INDEX = "sovecloud-object-meta"


def run_cmd(cmd, timeout=60):
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as exc:
        raise Fail("命令不存在: %s" % cmd[0]) from exc
    except subprocess.TimeoutExpired as exc:
        raise Fail("命令超时: %s" % " ".join(cmd)) from exc
    return proc.returncode, proc.stdout or "", proc.stderr or ""


def parse_json_text(text):
    if not text:
        raise Fail("没有返回 JSON")
    start = None
    for i, ch in enumerate(text):
        if ch in "{[":
            start = i
            break
    if start is None:
        raise Fail("没有返回 JSON")
    try:
        return json.loads(text[start:])
    except json.JSONDecodeError as exc:
        raise Fail("没有返回 JSON") from exc


def rgw_json(args, run=run_cmd):
    code, out, err = run(["radosgw-admin", "-f", "json", *args], timeout=60)
    if code != 0:
        raise Fail((err or out or "radosgw-admin 失败").strip())
    return parse_json_text(out)


def http_json(method, url, body=None, run=run_cmd):
    cmd = ["curl", "-sS", "-m", "20", "-X", method, "-w", "\n__SCES_%{http_code}", url]
    tmp_name = None
    try:
        if body is not None:
            fd, tmp_name = tempfile.mkstemp(prefix="sces-")
            os.write(fd, json.dumps(body).encode("utf-8"))
            os.close(fd)
            cmd[1:1] = ["-H", "Content-Type: application/json", "--data-binary", "@" + tmp_name]
        code, out, err = run(cmd, timeout=30)
    finally:
        if tmp_name:
            try:
                os.remove(tmp_name)
            except OSError:
                pass
    if code != 0:
        return None, None
    if "\n__SCES_" not in out:
        return None, None
    payload, status = out.rsplit("\n__SCES_", 1)
    status = status.strip()
    if not status.isdigit():
        return None, None
    if payload.strip() == "":
        return int(status), None
    try:
        return int(status), json.loads(payload)
    except json.JSONDecodeError:
        return int(status), None


def zones_of(zonegroup):
    if isinstance(zonegroup, dict) and "zones" in zonegroup:
        return zonegroup.get("zones") or []
    if isinstance(zonegroup, list):
        return zonegroup
    return []


def es_zone_name(zonegroup):
    for zone in zones_of(zonegroup):
        if isinstance(zone, dict) and zone.get("tier_type") == "elasticsearch":
            return zone.get("name")
    return None


def business_endpoints(zonegroup):
    found = []
    for zone in zones_of(zonegroup):
        if not isinstance(zone, dict):
            continue
        if zone.get("tier_type"):
            continue
        for ep in zone.get("endpoints") or []:
            found.append(ep)
    return found


def pool_map(zone):
    pools = {}
    if not isinstance(zone, dict):
        return pools
    for key, value in zone.items():
        if key == "domain_root" or key.endswith("_pool"):
            if isinstance(value, str):
                pools[key] = value
    return pools


def placement_pools(doc):
    if isinstance(doc, list):
        doc = doc[0] if doc else {}
    if not isinstance(doc, dict):
        return {"index_pool": None, "data_pool": None}
    return {
        "index_pool": doc.get("index_pool"),
        "data_pool": doc.get("data_pool"),
    }


def zone_get(run=run_cmd):
    zonegroup = rgw_json(["zonegroup", "get"], run=run)
    name = es_zone_name(zonegroup)
    business = business_endpoints(zonegroup)
    if not name:
        return {
            "deployed": False,
            "search_zone": None,
            "endpoint": None,
            "endpoints": [],
            "pools": {},
            "placement": None,
            "business_endpoints": business,
        }
    zone = rgw_json(["zone", "get", "--rgw-zone=" + name], run=run)
    tier = zone.get("tier_config") or {}
    placement = rgw_json([
        "zone", "placement", "get",
        "--rgw-zone=" + name,
        "--placement-id=default-placement",
    ], run=run)
    return {
        "deployed": True,
        "search_zone": name,
        "endpoint": tier.get("endpoint") or None,
        "endpoints": list(zone.get("endpoints") or []),
        "pools": pool_map(zone),
        "placement": placement_pools(placement),
        "business_endpoints": business,
    }


def endpoint_of(run=run_cmd):
    info = zone_get(run=run)
    if not info["deployed"]:
        return None
    return info["endpoint"]


def _as_int(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return value


def flatten(obj, prefix=""):
    if isinstance(obj, dict):
        for key, value in obj.items():
            path = prefix + "." + key if prefix else key
            if isinstance(value, dict):
                yield from flatten(value, path)
            else:
                yield path, value


def pick_setting(doc, suffix):
    if not isinstance(doc, dict):
        return None
    rank = {"transient": 0, "persistent": 1, "defaults": 2}
    found = []
    for path, value in flatten(doc):
        if path.endswith(suffix):
            scope = path.split(".", 1)[0]
            found.append((rank.get(scope, 9), value))
    if not found:
        return None
    found.sort(key=lambda item: item[0])
    return found[0][1]


def parse_table(text):
    rows = []
    for line in (text or "").splitlines():
        cols = line.split()
        if cols:
            rows.append(cols)
    return rows


def parse_nodes(text):
    nodes = []
    for cols in parse_table(text):
        if len(cols) < 5:
            continue
        nodes.append({
            "name": cols[0],
            "ip": cols[1],
            "node_role": cols[2],
            "heap_max": cols[3],
            "master": cols[4] == "*",
        })
    return nodes


def parse_allocation(text):
    found = {}
    for cols in parse_table(text):
        if len(cols) < 2:
            continue
        percent = None
        if len(cols) >= 3 and cols[2] not in ("", "-"):
            try:
                percent = int(float(cols[2]))
            except ValueError:
                percent = None
        found[cols[0]] = percent
        if len(cols) >= 2:
            found.setdefault(cols[1], percent)
    return found


def merge_nodes(node_text, alloc_text):
    nodes = parse_nodes(node_text)
    alloc = parse_allocation(alloc_text)
    for node in nodes:
        percent = alloc.get(node["name"])
        if percent is None:
            percent = alloc.get(node["ip"])
        node["disk_percent"] = percent
    return nodes


def parse_shards(text):
    rows = []
    for line in (text or "").splitlines():
        cols = line.split()
        if len(cols) < 3:
            continue
        node = " ".join(cols[3:]).strip() or None
        rows.append({
            "shard": cols[0],
            "prirep": cols[1],
            "state": cols[2],
            "node": node,
        })
    return rows


def parse_recovery(text):
    rows = []
    for cols in parse_table(text):
        if len(cols) < 5:
            continue
        rows.append({
            "index": cols[0],
            "shard": cols[1],
            "stage": cols[2],
            "source_node": cols[3],
            "target_node": cols[4],
        })
    return rows


def _unreachable():
    return {
        "es_reachable": False,
        "version": None,
        "health": None,
        "minimum_master_nodes": None,
        "watermark": None,
        "index": None,
    }


def cluster_get(run=run_cmd):
    base = endpoint_of(run=run)
    if not base:
        return _unreachable()
    base = base.rstrip("/")
    status, root = http_json("GET", base + "/", run=run)
    status_h, health = http_json("GET", base + "/_cluster/health", run=run)
    if not health or status_h != 200:
        return _unreachable()
    version = None
    if isinstance(root, dict):
        version = (root.get("version") or {}).get("number")
    settings_url = base + "/_cluster/settings?include_defaults=true&filter_path=**.minimum_master_nodes,**.watermark.low,**.watermark.high,**.watermark.flood_stage"
    _, settings = http_json("GET", settings_url, run=run)
    _, index_doc = http_json("GET", base + "/" + INDEX + "/_settings", run=run)
    count_status, count_doc = http_json("GET", base + "/" + INDEX + "/_count", run=run)
    index = {"name": INDEX, "exists": False, "count": None, "number_of_shards": None, "number_of_replicas": None}
    if isinstance(index_doc, dict) and INDEX in index_doc:
        idx = ((index_doc.get(INDEX) or {}).get("settings") or {}).get("index") or {}
        index["exists"] = True
        index["number_of_shards"] = _as_int(idx.get("number_of_shards"))
        index["number_of_replicas"] = _as_int(idx.get("number_of_replicas"))
    if count_status == 200 and isinstance(count_doc, dict) and "count" in count_doc:
        index["exists"] = True
        index["count"] = count_doc.get("count")
    watermark = None
    minimum = None
    if isinstance(settings, dict):
        minimum = _as_int(pick_setting(settings, "minimum_master_nodes"))
        low = pick_setting(settings, "watermark.low")
        high = pick_setting(settings, "watermark.high")
        flood = pick_setting(settings, "watermark.flood_stage")
        if low is not None or high is not None or flood is not None:
            watermark = {"low": low, "high": high, "flood_stage": flood}
    return {
        "es_reachable": True,
        "version": version,
        "health": {
            "status": health.get("status"),
            "number_of_nodes": health.get("number_of_nodes"),
            "active_shards": health.get("active_shards"),
            "unassigned_shards": health.get("unassigned_shards"),
            "relocating_shards": health.get("relocating_shards"),
        },
        "minimum_master_nodes": minimum,
        "watermark": watermark,
        "index": index,
    }


def cat(base, path, run):
    status, _ = None, None
    code, out, err = run(["curl", "-sS", "-m", "20", "-w", "\n__SCES_%{http_code}", base + path], timeout=30)
    if code != 0 or "\n__SCES_" not in out:
        return None
    payload, status_text = out.rsplit("\n__SCES_", 1)
    if not status_text.strip().isdigit() or int(status_text) != 200:
        return None
    return payload


def node_list(run=run_cmd):
    base = endpoint_of(run=run)
    if not base:
        return {"es_reachable": False, "nodes": []}
    base = base.rstrip("/")
    nodes_text = cat(base, "/_cat/nodes?h=name,ip,node.role,heap.max,master", run)
    if nodes_text is None:
        return {"es_reachable": False, "nodes": []}
    alloc_text = cat(base, "/_cat/allocation?h=node,ip,disk.percent", run) or ""
    return {"es_reachable": True, "nodes": merge_nodes(nodes_text, alloc_text)}


def shard_get(run=run_cmd):
    base = endpoint_of(run=run)
    if not base:
        return {"es_reachable": False, "index_exists": False, "shards": [], "recovery": []}
    base = base.rstrip("/")
    shard_text = cat(base, "/_cat/shards/%s?h=shard,prirep,state,node" % INDEX, run)
    if shard_text is None:
        # 404 表示索引还没有，9200 本身是通的时 cat 会返回非 200。再看 health 区分连不上和索引不存在。
        health = cat(base, "/_cluster/health", run)
        if health is None:
            return {"es_reachable": False, "index_exists": False, "shards": [], "recovery": []}
        return {"es_reachable": True, "index_exists": False, "shards": [], "recovery": []}
    recovery_text = cat(base, "/_cat/recovery/%s?active_only=true&h=index,shard,stage,source_node,target_node" % INDEX, run) or ""
    return {
        "es_reachable": True,
        "index_exists": True,
        "shards": parse_shards(shard_text),
        "recovery": parse_recovery(recovery_text),
    }


def put_settings(body, run=run_cmd):
    base = endpoint_of(run=run)
    if not base:
        raise Fail("读不到检索服务地址")
    status, doc = http_json("PUT", base.rstrip("/") + "/_cluster/settings", body, run=run)
    if status is None or status >= 300:
        raise Fail("写集群设置失败")
    return doc


def exclude_set(ip, run=run_cmd):
    with locked("/", "sces-exclude.lock"):
        put_settings({"transient": {"cluster.routing.allocation.exclude._ip": ip}}, run=run)
    return {"exclude_ip": ip, "files": []}


def exclude_clear(run=run_cmd):
    with locked("/", "sces-exclude.lock"):
        put_settings({"transient": {"cluster.routing.allocation.exclude._ip": None}}, run=run)
    return {"exclude_ip": None, "files": []}


def watermark_apply(run=run_cmd):
    body = {"persistent": {
        "cluster.routing.allocation.disk.watermark.low": "70%",
        "cluster.routing.allocation.disk.watermark.high": "80%",
        "cluster.routing.allocation.disk.watermark.flood_stage": "85%",
    }}
    with locked("/", "sces-watermark.lock"):
        put_settings(body, run=run)
    return {"watermark": {"low": "70%", "high": "80%", "flood_stage": "85%"}, "files": []}


def clear_readonly(run=run_cmd):
    base = endpoint_of(run=run)
    if not base:
        raise Fail("读不到检索服务地址")
    body = {"index": {"blocks": {"read_only_allow_delete": None}}}
    with locked("/", "sces-index.lock"):
        status, _doc = http_json("PUT", base.rstrip("/") + "/" + INDEX + "/_settings", body, run=run)
    if status is None or status >= 300:
        raise Fail("去掉只读失败")
    return {"index": INDEX, "files": []}


def apply_zone_spec(zone, spec):
    if not isinstance(zone, dict):
        raise Fail("zone get 没有返回对象")
    endpoint = spec.get("endpoint")
    endpoints = spec.get("endpoints")
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise Fail("endpoint 不合法")
    if not isinstance(endpoints, list) or not endpoints or not all(isinstance(x, str) and x.strip() for x in endpoints):
        raise Fail("endpoints 必须是非空的地址列表")
    tier = zone.get("tier_config")
    if not isinstance(tier, dict):
        tier = {}
        zone["tier_config"] = tier
    tier["endpoint"] = endpoint.strip()
    current = tier.get("explicit_custom_meta", "false")
    tier["explicit_custom_meta"] = False if isinstance(current, bool) else "false"
    zone["endpoints"] = [x.strip() for x in endpoints]
    return zone


def zone_set(spec, run=run_cmd):
    if not isinstance(spec, dict):
        raise Fail("规格必须是 JSON 对象")
    extra = [k for k in spec if k not in ("endpoint", "endpoints")]
    if extra:
        raise Fail("规格里有不能写的字段: %s" % ", ".join(extra))
    with locked("/", "sces-zone.lock"):
        zonegroup = rgw_json(["zonegroup", "get"], run=run)
        name = es_zone_name(zonegroup)
        if not name:
            raise Fail("还没有检索域")
        zone = rgw_json(["zone", "get", "--rgw-zone=" + name], run=run)
        zone = apply_zone_spec(zone, spec)
        fd, tmp_name = tempfile.mkstemp(prefix="sces-zone-")
        os.close(fd)
        try:
            atomic_write(tmp_name, json.dumps(zone, ensure_ascii=False))
            code, out, err = run(["radosgw-admin", "-f", "json", "zone", "set", "--rgw-zone=" + name, "--infile=" + tmp_name], timeout=60)
            if code != 0:
                raise Fail((err or out or "zone set 失败").strip())
        finally:
            try:
                os.remove(tmp_name)
            except OSError:
                pass
    return {"endpoint": zone["tier_config"]["endpoint"], "endpoints": zone["endpoints"], "files": []}
