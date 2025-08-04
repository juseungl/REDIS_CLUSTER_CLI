import sys
import time
import redis
from utils.string_utils import StringUtils
from utils.print_utils import PrintUtils
from utils.redis_utils import RedisUtils


def add_node(new_node, existing_node, master_id=None, password=None):
    """
    Redis 클러스터에 새 노드를 추가하는 함수입니다.

    - new_node: 새로 추가할 노드 (ip:port)
    - existing_node: 이미 클러스터에 포함된 노드 (ip:port)
    - master_id: 새 노드를 복제 노드로 등록할 마스터 노드 ID (없으면 새 노드를 마스터로 등록)
    - password: Redis 접속 비밀번호 (필요하면 전달)

    처리 순서:
    1) 기존 노드에 새 노드의 CLUSTER MEET 요청으로 클러스터에 참가시킴
    2) 새 노드가 클러스터에 잘 등록됐는지 확인 (node_id 조회)
    3) master_id가 있으면 새 노드를 해당 마스터의 리플리카로 지정 (복제 설정)
    4) 없으면 새 노드는 마스터로 남음
    """

    # 1. ip, port 분리 및 Redis 연결 생성
    new_host, new_port = StringUtils.parse_node(new_node)
    ex_host, ex_port = StringUtils.parse_node(existing_node)

    ex_redis = RedisUtils.connect_node(ex_host, ex_port, password)
    new_redis = RedisUtils.connect_node(new_host, new_port, password)

    # 2. 기존 노드에 CLUSTER MEET 명령으로 새 노드가 클러스터에 참여하도록 요청
    try:
        ex_redis.execute_command("CLUSTER MEET", new_host, new_port)
        print(f"🔗 기존 노드 {ex_host}:{ex_port} 에서 새 노드 {new_host}:{new_port} 로 CLUSTER MEET 요청 성공")
    except redis.exceptions.ResponseError as e:
        print(f"❌ CLUSTER MEET 실패: {e}")
        sys.exit(1)

    # 3. 새 노드가 클러스터에 완전히 등록될 때까지 최대 10초 대기하며 확인
    print("⏳ 새 노드가 클러스터에 등록될 때까지 대기 중...")
    node_id = None
    for _ in range(10):
        try:
            nodes_info = new_redis.execute_command("CLUSTER NODES")

            if isinstance(nodes_info, str):
                for line in nodes_info.splitlines():
                    if "myself" in line:
                        node_id = line.split()[0]
                        print(f"✅ 새 노드 {new_host}:{new_port} 클러스터 참여 확인 (node_id: {node_id})")
                        break

            elif isinstance(nodes_info, dict):
                for addr, node in nodes_info.items():
                    flags_str = node.get("flags", "")
                    flags = [f.strip() for f in flags_str.split(",")]
                    if "myself" in flags:
                        node_id = node.get("node_id")
                        print(f"✅ 새 노드 {new_host}:{new_port} 클러스터 참여 확인 (node_id: {node_id})")
                        break

            else:
                print(f"⚠️ CLUSTER NODES 명령이 예상치 못한 타입을 반환: {type(nodes_info)}")

        except Exception as e:
            print(f"❗ 예외 발생: {e}")

        if node_id:
            break
        time.sleep(1)
    else:
        print("❌ 새 노드가 클러스터에 참여하지 않았습니다.")
        sys.exit(1)



    # 4. master_id가 주어졌다면 새 노드를 해당 마스터의 리플리카로 지정
    if master_id:
        print(f"🔁 master_id({master_id})가 있으므로 새 노드를 복제 노드로 지정합니다.")
        try:
            # 클러스터 안정화를 위해 잠시 대기 후 실행
            time.sleep(2)
            new_redis.execute_command("CLUSTER REPLICATE", master_id)
            print(f"🔁 새 노드 {new_host}:{new_port} 를 마스터 {master_id} 의 리플리카로 지정 성공")
        except redis.exceptions.ResponseError as e:
            print(f"❌ CLUSTER REPLICATE 실패: {e}")
            sys.exit(1)
    else:
        print(f"🆕 master_id가 없으므로 새 노드 {new_host}:{new_port} 는 마스터로 남습니다.")

    print("🎉 노드 추가 완료")