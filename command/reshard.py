import redis
import sys
import time
from tqdm import tqdm
from utils.string_utils import StringUtils
from utils.redis_utils import RedisUtils

def reshard(from_id, to_id, slots, pipeline, access_node, password):
    """
    지정된 슬롯 수만큼 from_id 노드에서 to_id 노드로 슬롯을 이동(리샤딩)하는 메인 함수.
    """
    print(f"🔍 {access_node}를 통해 클러스터에 연결 중...")
    ip, port = StringUtils.parse_node(access_node)
    r = RedisUtils.connect_node(ip, port, password)

    nodes_dict = RedisUtils.cluster_nodes(r)
    validate_from_to_nodes(nodes_dict, from_id, to_id, slots)

    node_id_to_addr = {info['node_id']: addr for addr, info in nodes_dict.items()}
    from_addr = node_id_to_addr[from_id]
    to_addr = node_id_to_addr[to_id]
    from_ip, from_port = from_addr.split(":")
    to_ip, to_port = to_addr.split(":")
    from_port = int(from_port)
    to_port = int(to_port)

    print(f"🔗 소스 노드: {from_ip}:{from_port}, 대상 노드: {to_ip}:{to_port}")

    from_conn = RedisUtils.connect_node(from_ip, from_port, password)
    to_conn = RedisUtils.connect_node(to_ip, to_port, password)

    available_slots = get_node_slots(nodes_dict, from_id)
    slots_to_move = available_slots[-slots:]  # 뒤에서 slots 개 만큼 선택

    print(f"🔀 슬롯 {slots}개를 노드 {from_addr} -> {to_addr} 로 이동 시작")

    for slot in tqdm(slots_to_move, desc="슬롯 이동 진행", unit="slot"):
        migrate_slot(from_conn, to_conn, slot, from_id, to_id, pipeline, to_ip, to_port, password)

    print("✅ 리샤딩 완료!")


def migrate_slot(from_conn, to_conn, slot, from_id, to_id, pipeline_size, to_host, to_port, password):
    """
    특정 슬롯에 속한 모든 키를 소스 노드에서 대상 노드로 MIGRATE함.
    """
    # 슬롯 상태를 각각 대상 노드에 IMPORTING, 소스 노드에 MIGRATING 으로 설정
    RedisUtils.set_slot_importing(to_conn, slot, from_id)
    RedisUtils.set_slot_migrating(from_conn, slot, to_id)

    # 슬롯 내 키를 pipeline_size 단위로 반복적으로 MIGRATE
    while True:
        keys = RedisUtils.get_keys_in_slot(from_conn, slot, pipeline_size)
        if not keys:
            break
        for key in keys:
            RedisUtils.migrate_key(from_conn, to_host, to_port, key, password, 60000)

    # 슬롯 소유권을 대상 노드로 변경
    RedisUtils.set_slot_node(to_conn, slot, to_id)
    RedisUtils.set_slot_node(from_conn, slot, from_id)


def validate_from_to_nodes(nodes_dict, from_id, to_id, slots):
    """
    from_id, to_id 노드 및 슬롯 이동 개수의 유효성 검사 수행.
    - 노드 존재 여부, 마스터 여부, 슬롯 보유 개수 등 체크.
    """
    errors = []
    warnings = []

    from_node = None
    to_node = None

    for addr, info in nodes_dict.items():
        if info['node_id'] == from_id:
            from_node = info
        if info['node_id'] == to_id:
            to_node = info

    if not from_node:
        errors.append(f"FROM 노드 {from_id}를 찾을 수 없습니다.")
    if not to_node:
        errors.append(f"TO 노드 {to_id}를 찾을 수 없습니다.")

    if from_node:
        if 'master' not in from_node['flags']:
            errors.append(f"FROM 노드 {from_id}는 마스터가 아닙니다.")
        if not from_node.get('slots'):
            errors.append(f"FROM 노드 {from_id}는 슬롯을 보유하고 있지 않습니다.")
        else:
            # 보유한 슬롯 개수 합산
            from_slots_count = 0
            for slot_range in from_node['slots']:
                if isinstance(slot_range, list):
                    start = int(slot_range[0])
                    end = int(slot_range[1])
                    from_slots_count += (end - start + 1)
                else:
                    from_slots_count += 1
            if from_slots_count < slots:
                errors.append(f"FROM 노드가 보유한 슬롯 개수({from_slots_count})가 이동 요청 슬롯 수({slots})보다 적습니다.")

    if to_node and 'master' not in to_node['flags']:
        errors.append(f"TO 노드 {to_id}는 마스터가 아닙니다.")

    if warnings:
        print("⚠️ 경고:")
        for warn in warnings:
            print(f" - {warn}")

    if errors:
        print("❌ 유효성 검사 실패:")
        for err in errors:
            print(f" - {err}")
        sys.exit(1)

    print("✅ FROM/TO 노드 유효성 검사 통과")


def get_node_slots(nodes_dict, from_id):
    """
    from_id 노드가 보유한 슬롯 번호 리스트 반환
    """
    for addr, info in nodes_dict.items():
        if info["node_id"] == from_id or info.get("id") == from_id:
            slots = []
            for slot_range in info.get("slots", []):
                if isinstance(slot_range, list):
                    if len(slot_range) == 2:
                        start, end = map(int, slot_range)
                        slots.extend(range(start, end + 1))
                    elif len(slot_range) == 1:
                        slots.append(int(slot_range[0]))
                else:
                    slots.append(int(slot_range))
            return slots
    return []