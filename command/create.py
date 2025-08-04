import redis
import sys
import time
from utils.string_utils import StringUtils
from utils.print_utils import PrintUtils
from utils.redis_utils import RedisUtils
from tqdm import tqdm

def create(nodes, replicas, password):
    """
    Redis Cluster 생성 메인 함수
    - 노드 연결 및 클러스터 토폴로지 구성
    - 마스터/리플리카 역할 분리 및 슬롯 할당
    - 리플리카에 마스터 할당(복제 설정)
    """
    one_set = replicas + 1  # 마스터 1 + 리플리카 수 = 한 세트
    num_masters = len(nodes) // one_set
    validate_master_count(num_masters, len(nodes))

    print("\n--- 1. 노드 연결 및 클러스터 토폴로지 구성 ---")
    conns = [RedisUtils.connect_node(*StringUtils.parse_node(n), password) for n in nodes]
    perform_cluster_meet(conns)

    print("\n--- 2. 마스터/리플리카 분리 및 슬롯 할당 ---")
    master_nodes, replica_nodes = split_and_print_nodes(conns, num_masters)
    assign_slots_to_masters(master_nodes, RedisUtils.TOTAL_SLOTS, num_masters)

    print("\n--- 3. 리플리카에 마스터 할당 (복제 설정) ---")
    master_ids = get_master_nodes_ids(master_nodes)
    assign_replicas_to_masters(replica_nodes, master_ids, master_nodes)

    print("\n⌛ 클러스터 안정화 대기 중...")
    for _ in tqdm(range(20), desc="    - 대기중", ncols=70):
        time.sleep(0.1)
    print("\n🎉 클러스터 생성 완료! 🎉")


def validate_master_count(num_masters, total_nodes):
    if total_nodes < 6:
        print("❌ 최소 6개 노드가 필요합니다.")
        sys.exit(1)
    if num_masters < 1 or num_masters > total_nodes:
        print("❌ 마스터 노드 수가 전체 노드 수보다 크거나 1보다 작을 수 없습니다.")
        sys.exit(1)


def perform_cluster_meet(nodes):
    """
    첫 번째 노드에 나머지 노드들 MEET 명령 전송해 클러스터 연결 형성
    """
    print("[노드 간 MEET 요청]")
    first_node = nodes[0]
    for n in nodes[1:]:
        host = n.connection_pool.connection_kwargs["host"]
        port = n.connection_pool.connection_kwargs["port"]
        try:
            RedisUtils.cluster_meet(first_node, host, port)
            print(f"    - {host}:{port} MEET 요청 성공")
        except redis.exceptions.ResponseError as e:
            print(f"❌ MEET 실패: {e}")
    
    print("\n⌛ MEET 전파 대기 중...")
    for _ in tqdm(range(20), desc="    - 대기중", ncols=70):
        time.sleep(0.1)
    print()


def split_and_print_nodes(conns, num_masters):
    """
    연결 리스트를 마스터/리플리카로 분리 후 정보 출력
    """
    master_nodes = conns[:num_masters]
    replica_nodes = conns[num_masters:]
    print("\n마스터 노드:")
    PrintUtils.print_nodes_info(master_nodes, "마스터")
    print("\n리플리카 노드:")
    PrintUtils.print_nodes_info(replica_nodes, "리플리카")
    return master_nodes, replica_nodes


def assign_slots_to_masters(master_nodes, total_slots, num_masters):
    """
    슬롯(0~16383)을 마스터 노드에 균등 분배 후 할당
    """
    print("\n[슬롯 할당]")
    slots_per_master = total_slots // num_masters
    remain = total_slots % num_masters
    current_slot = 0

    for i, master in enumerate(master_nodes):
        count = slots_per_master + (1 if i < remain else 0)
        slots = list(range(current_slot, current_slot + count))
        try:
            RedisUtils.cluster_add_slots(master, slots)
            print(f"    - {PrintUtils.node_str(master)} → 슬롯 {slots[0]} ~ {slots[-1]} 할당 완료")
        except redis.exceptions.ResponseError as e:
            if "already busy" in str(e):
                print(f"    - ⚠️ {PrintUtils.node_str(master)}: 이미 슬롯 할당됨, 건너뜀")
            else:
                raise e
        current_slot += count


def get_master_nodes_ids(master_nodes):
    """
    마스터 노드들의 클러스터 ID 조회
    """
    master_ids = []
    print("\n[마스터 노드 ID 조회]")
    for i, master in enumerate(master_nodes):
        master_id = RedisUtils.cluster_myid(master)
        master_ids.append(master_id)
        print(f"    {i+1}. {PrintUtils.node_str(master)} → ID: {master_id}")
    return master_ids


def assign_replicas_to_masters(replica_nodes, master_ids, master_nodes):
    """
    각 리플리카 노드를 마스터에 연결해 복제 관계 설정
    """
    print("\n[리플리카 복제 설정]")
    for idx, replica in enumerate(replica_nodes):
        master_id = master_ids[idx % len(master_ids)]
        try:
            RedisUtils.cluster_replica(replica, master_id)
            print(f"    - {PrintUtils.node_str(replica)} → {PrintUtils.node_str(master_nodes[idx % len(master_nodes)])}에 복제 설정 완료")
        except redis.exceptions.ResponseError as e:
            print(f"❌ 복제 설정 실패: {e}")