import sys
import redis
import ast
from utils.string_utils import StringUtils
from utils.print_utils import PrintUtils
from utils.redis_utils import RedisUtils


def check(access_node, password):
    """
    Redis 클러스터 상태 점검의 메인 함수.
    1단계부터 4단계까지 클러스터 노드 정보 파싱, 슬롯 커버리지, 연결 상태, 노드간 정보 일치성 검사를 수행.
    """ 
    # redis 연결 객체
    r = connect_base_node(access_node, password)
    # 클러스터에 포함된 노드 정보 추출 dict
    nodes_dict = fetch_cluster_nodes(r)
    # 비교 위해 정규화 (불필요한 node 정보 제거)
    print("\n CLUSTER NODES로 노드 정보 정규화(불필요한 필드 제거, 정렬 등)...\n ")
    normalized_nodes = normalize_nodes(nodes_dict)


    # 1. 슬롯 커버리지 체크
    slot_check = check_slot_coverage(normalized_nodes)
    # 2. 연결 상태 체크(cluster_nodes로 얻은 정보에서 connection 확인)
    connected_check = check_node_connections(normalized_nodes)
    # 3. 모든 노드가 동일한 CLUSTER NODES를 반환하는가 체크
    cluster_consistency = check_cluster_consistency(normalized_nodes, password, nodes_dict)

    # 결과 출력
    print_summary(slot_check, connected_check, cluster_consistency, r)


def connect_base_node(access_node, password):
    """
    기준 노드에 연결하여 Redis 인스턴스를 반환한다.
    """
    host, port = StringUtils.parse_node(access_node)
    print(f"🔍 기준 노드 {access_node} 에 연결 중...")
    return RedisUtils.connect_node(host, port, password)


def fetch_cluster_nodes(redis_client):
    """
    Redis 'CLUSTER NODES' 명령을 실행하여 노드 정보를 가져온다.
    실패시 프로세스 종료.
    """
    try:
        print("\n CLUSTER NODES로 노드 정보 추출...")
        return RedisUtils.cluster_nodes(redis_client)
    except redis.exceptions.RedisError as e:
        print(f"❌ CLUSTER NODES 명령 실행 실패: {e}")
        sys.exit(1)


def normalize_nodes(nodes):
    """
    노드 딕셔너리를 받아, 각 노드의 flags 정리, slots 문자열 통일, 
    불필요한 플래그 제거 후 addr 기준으로 정렬하여 반환한다.
    """
    normalized = {}
    for addr, info in nodes.items():
        node_id = info.get("node_id")
        raw_flags = info.get("flags", [])
        if isinstance(raw_flags, str):
            flags_list = raw_flags.split(",")
        else:
            flags_list = list(raw_flags)
        flags_list = [f for f in flags_list if f != "myself"]
        flags = ",".join(sorted(flags_list))

        master_id = info.get("master_id")
        slots = slots_to_str(info.get("slots", []))
        connected = info.get("connected", False)

        normalized[addr] = {
            "node_id": node_id,
            "flags": flags,
            "master_id": master_id,
            "slots": slots,
            "connected": connected,
            "addr": addr,
        }
    # addr 키 기준 정렬 후 반환
    return dict(sorted(normalized.items(), key=lambda x: x[0]))


def slots_to_str(slots):
    """
    slots 리스트를 받아서, 내부가 리스트이면 "start-end" 형식 문자열로 변환,
    문자열이면 그대로 사용. 정렬된 리스트로 반환.
    """
    result = []
    for slot in slots:
        if isinstance(slot, list) and len(slot) == 2:
            result.append(f"{slot[0]}-{slot[1]}")
        else:
            result.append(str(slot))
    return sorted(result)


def check_slot_coverage(normalized_nodes):
    """
    모든 노드의 슬롯 커버리지를 합쳐 0~16383 슬롯이 전부 커버되는지 검사.
    누락된 슬롯이 있으면 False, 모두 있으면 True 반환.
    """
    print("📦 [첫 번째] 슬롯 커버리지 확인 중...\n")

    all_slots = set()

    for addr, info in normalized_nodes.items():
        for slot_range in info["slots"]:
            # slot_range 가 문자열로 표현된 리스트 형태면 ast.literal_eval 로 변환 시도
            if isinstance(slot_range, str) and slot_range.startswith("["):
                try:
                    slot_range = ast.literal_eval(slot_range)
                except Exception:
                    print(f"⚠️ 슬롯 파싱 실패: {slot_range}")
                    continue

            if isinstance(slot_range, list):
                if len(slot_range) == 2:
                    all_slots.update(range(int(slot_range[0]), int(slot_range[1]) + 1))
                elif len(slot_range) == 1:
                    all_slots.add(int(slot_range[0]))
            elif isinstance(slot_range, str) and "-" in slot_range:
                start, end = map(int, slot_range.split("-"))
                all_slots.update(range(start, end + 1))
            else:
                try:
                    all_slots.add(int(slot_range))
                except ValueError:
                    print(f"⚠️ 처리할 수 없는 슬롯: {slot_range}")

    missing_slots = set(range(16384)) - all_slots
    if missing_slots:
        print(f"⚠️ 할당되지 않은 슬롯 존재: 총 {len(missing_slots)}개 슬롯가 할당되지 않았습니다.\n")
        return False
    else:
        print("✅ 모든 슬롯이 정상적으로 할당되어 있습니다.\n")
        return True


def check_node_connections(normalized_nodes):
    """
    모든 노드의 연결 상태를 확인.
    disconnected 노드가 있으면 False, 모두 연결되어 있으면 True 반환.
    """
    print("🔌 [두 번째] 노드 연결 상태 확인 중...\n")

    connected_check = True
    for addr, info in normalized_nodes.items():
        if not info["connected"]:
            print(f"❌ 노드 {addr} 연결 상태: disconnected")
            connected_check = False
        else:
            print(f"✅ 노드 {addr} 연결 상태: connected")

    print()
    return connected_check


def check_cluster_consistency(normalized_nodes, password, nodes_dict):
    """
    각 노드에 접속해 CLUSTER NODES 정보를 가져와 기준 정보(normalized_nodes)와 비교.
    일치하지 않으면 그 노드 주소를 리스트에 추가해 반환.
    """
    print("🧩 [세 번째] CLUSTER NODES 정보 일치 여부 검사 중...\n")

    inconsistent_nodes = []

    for addr, info in normalized_nodes.items():
        h, p = StringUtils.parse_node(addr)
        node_r = RedisUtils.connect_node(h, p, password)
        try:
            cluster_nodes_at_node = RedisUtils.cluster_nodes(node_r)
            if isinstance(cluster_nodes_at_node, dict):
                normalized_at_node = normalize_nodes(cluster_nodes_at_node)

                if normalized_at_node != normalized_nodes:
                    inconsistent_nodes.append(addr)
            else:
                raw = node_r.execute_command("CLUSTER NODES")
                if str(raw).strip() != str(nodes_dict).strip():
                    inconsistent_nodes.append(addr)
        except Exception as e:
            print(f"⚠️ 노드 {addr} 에서 비교 실패: {e}")
            inconsistent_nodes.append(addr)

    if inconsistent_nodes:
        print(f"❌ CLUSTER NODES 정보가 일치하지 않는 노드: {len(inconsistent_nodes)}개")
        for addr in inconsistent_nodes:
            print(f" - {addr}")
        return False
    else:
        print("✅ 모든 노드가 동일한 CLUSTER NODES 정보를 가지고 있습니다.")
        return True


import pprint
def print_summary(slot_check, connected_check, cluster_consistency, r):
    """
    점검 결과를 요약하여 출력한다.
    """
    print("\n🧾 [최종 점검 결과 요약]")
    print(f" - 슬롯 커버리지: {'✅ 정상' if slot_check else '⚠️ 누락 있음'}")
    print(f" - 노드 연결 상태: {'✅ 모두 연결됨' if connected_check else '❌ 연결 끊긴 노드 있음'}")
    print(f" - CLUSTER NODES 일치성: {'✅ 일치함' if cluster_consistency else '❌ 불일치함'}")
    print("\n🧾클러스터에 포함된 노드 정보 출력)")
    pprint.pprint(RedisUtils.cluster_nodes(r))
    if connected_check and cluster_consistency:
        print("\n🎉 클러스터 상태는 정상입니다.")
    else:
        print("\n⚠️ 클러스터에 이상이 있습니다. 조치가 필요합니다.")
