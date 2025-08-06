import sys
import redis
from utils.string_utils import StringUtils
from utils.print_utils import PrintUtils
from utils.redis_utils import RedisUtils

def del_node(access_node, node_id_to_remove, password):
    """
    클러스터에서 특정 노드를 삭제하는 메인 함수
    - access_node: 클러스터에 접속하기 위한 임의 노드 (ip:port)
    - node_id_to_remove: 삭제할 node's ID
    - password: Redis 인증 비밀번호
    """
    host, port = StringUtils.parse_node(access_node)
    PrintUtils.info(f"1. {host}:{port} 노드에 연결 중...\n")
    connection = RedisUtils.connect_node(host, port, password)

    nodes_dict = get_cluster_nodes(connection)
    validate_node_exists(nodes_dict, node_id_to_remove)
    forget_node_from_cluster(password, nodes_dict, node_id_to_remove)

    PrintUtils.success("노드 삭제 작업 완료. 클러스터 상태를 확인하세요.\n")


def get_cluster_nodes(connection):
    """
    클러스터 내 모든 노드 정보 조회
    """
    try:
        return RedisUtils.cluster_nodes(connection)
    except redis.exceptions.RedisError as e:
        PrintUtils.error(f"CLUSTER NODES 명령 실패: {e}\n")
        sys.exit(1)


def validate_node_exists(nodes_dict, node_id):
    """
    삭제할 노드가 클러스터에 존재하는지 확인
    """
    if not any(info['node_id'] == node_id for info in nodes_dict.values()):
        PrintUtils.error(f"삭제할 노드 ID {node_id}가 클러스터에 없습니다.\n")
        sys.exit(1)
    PrintUtils.success(f"삭제할 노드 ID {node_id}가 클러스터에 존재합니다.\n")


def get_remove_target_node_connection(password, nodes_dict, node_id):
    """
    삭제 대상 노드의 Redis 연결과 주소 반환
    """
    for addr, info in nodes_dict.items():
        if info['node_id'] == node_id:
            host, port = StringUtils.parse_node(addr)
            return RedisUtils.connect_node(host, port, password), addr
    PrintUtils.error(f"{node_id}에 해당하는 노드 주소를 찾을 수 없습니다.\n")
    return None, None


def forget_node_from_cluster(password, nodes_dict, node_id_to_remove):
    """
    모든 노드에서 해당 노드를 FORGET 처리하고,
    삭제 대상 노드는 CLUSTER RESET 하여 클러스터에서 완전히 분리
    """
    # 1. 삭제 대상 노드가 마스터일 경우, 복제본 노드가 있는지 확인
    dependent_replicas = [addr for addr, info in nodes_dict.items() if info.get("master_id") == node_id_to_remove]
    if dependent_replicas:
        PrintUtils.warn(
            f"⚠️ 삭제 대상 노드({node_id_to_remove})는 다음 노드들의 마스터입니다:\n"
            + "\n".join([f"   - {addr}" for addr in dependent_replicas]) +
            "\n\n복제본 노드를 먼저 제거하거나 재구성 후 다시 시도하세요.\n"
        )
        sys.exit(1)

    # 2. 클러스터 내 모든 노드에 FORGET 명령 전파
    PrintUtils.step("클러스터 모든 노드에 FORGET 명령 전달 중...\n")
    for addr, info in nodes_dict.items():
        if info['node_id'] == node_id_to_remove:
            continue  # 삭제 대상 노드는 나중에 처리
        host, port = StringUtils.parse_node(addr)
        node_conn = RedisUtils.connect_node(host, port, password)
        try:
            RedisUtils.cluster_forget(node_conn, node_id_to_remove)
            PrintUtils.success(f"{addr} 에서 {node_id_to_remove} FORGET 성공")
        except redis.exceptions.RedisError as e:
            PrintUtils.warn(f"{addr} 에서 {node_id_to_remove} FORGET 실패: {e}")

    # 3. 삭제 대상 노드는 RESET 하여 클러스터에서 완전 분리
    print()
    PrintUtils.step(f"삭제 대상 노드({node_id_to_remove})를 클러스터에서 분리 중...\n")
    target_conn, target_addr = get_remove_target_node_connection(password, nodes_dict, node_id_to_remove)
    if target_conn:
        try:
            RedisUtils.cluster_reset(target_conn)
            PrintUtils.success(f"{target_addr} 노드를 RESET하여 클러스터에서 분리했습니다.\n")
        except redis.exceptions.RedisError as e:
            PrintUtils.warn(f"{target_addr} 노드 RESET 실패: {e}\n이미 분리되었는지 확인하세요.\n")