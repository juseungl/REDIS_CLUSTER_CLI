import sys
import time
import redis
from utils.string_utils import StringUtils
from utils.redis_utils import RedisUtils
from tqdm import tqdm

def add_node(new_node, existing_node, password, master_id=None):
    """
    Redis Cluster에 새로운 노드를 추가하는 함수

    (인자)
    - new_node (str): 새로 추가할 노드의 주소 (ip:port)
    - existing_node (str): 클러스터에 이미 참여중인 노드 주소 (ip:port)
    - password (str): Redis 인증 비밀번호
    - master_id (str, optional): 새 노드를 리플리카로 지정할 마스터 노드 ID (없으면 마스터로 유지)
    """
    # 노드 주소 파싱
    new_host, new_port = StringUtils.parse_node(new_node)
    exist_host, exist_port = StringUtils.parse_node(existing_node)

    # Redis 연결 생성
    exist_redis = RedisUtils.connect_node(exist_host, exist_port, password)
    new_redis = RedisUtils.connect_node(new_host, new_port, password)

    # 기존 클러스터 노드에 새 노드 MEET 요청으로 클러스터에 합류
    join_cluster(exist_redis, new_host, new_port)

    # master_id가 있으면 리플리카로 설정, 없으면 마스터로 유지
    if master_id:
        assign_as_replica(new_redis, new_host, new_port, master_id)
    else:
        keep_as_master(new_host, new_port)

    # 새 노드의 클러스터 내 고유 ID 조회 및 출력
    node_id = RedisUtils.cluster_myid(new_redis)
    print(f"🎉 노드 추가 완료: {new_node} (Node ID: {node_id})\n")


def join_cluster(exist_redis, new_host, new_port):
    """
    클러스터 참여 중인 노드에 MEET 명령어를 보내 새 노드를 클러스터에 참여시킴
    """
    print("\n🔗 기존 클러스터 노드에 MEET 요청 중...")
    try:
        RedisUtils.cluster_meet(exist_redis, new_host, new_port)
        print(f"    - {new_host}:{new_port} 에 대해 MEET 요청 성공")
    except redis.exceptions.ResponseError as e:
        print(f"❌ MEET 요청 실패: {e}")
        sys.exit(1)

    # 명령 전파를 위한 잠시 대기
    print("\n⌛ MEET 명령 전파 대기 중 (약 2초)...")
    for _ in tqdm(range(20), desc="    - 대기중", ncols=70):
        time.sleep(0.1)
    print()


def assign_as_replica(new_redis, new_host, new_port, master_id):
    """
    새 노드를 지정한 마스터 노드의 리플리카로 설정
    """
    print(f"🔁 새 노드를 마스터 {master_id}의 리플리카로 지정합니다.\n")
    try:
        RedisUtils.cluster_replica(new_redis, master_id)
        print(f"    - 리플리카 설정 완료: {new_host}:{new_port} → {master_id}\n")
    except redis.exceptions.ResponseError as e:
        print(f"❌ 리플리카 설정 실패: {e}")
        sys.exit(1)


def keep_as_master(new_host, new_port):
    """
    master_id가 없을 경우, 새 노드를 마스터로 유지함
    """
    print(f"🔁 master_id 미지정으로 새 노드 {new_host}:{new_port} 는 마스터로 유지됩니다.\n")
