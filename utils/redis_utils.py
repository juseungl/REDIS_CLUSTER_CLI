import redis
import sys
from utils.print_utils import PrintUtils

class RedisUtils:
    CLUSTER_MEET = "CLUSTER MEET"
    TOTAL_SLOTS = 16384  # Redis 클러스터에서 사용할 수 있는 총 슬롯 개수
    CLUSTER_MYID = "CLUSTER MYID"
    CLUSTER_REPLICATE = "CLUSTER REPLICATE"
    CLUSTER_ADDSLOTS = "CLUSTER ADDSLOTS"
    CLUSTER_NODES = "CLUSTER NODES"
    CLUSTER_FORGET = "CLUSTER FORGET"
    CLUSTER_RESET = "CLUSTER RESET"
    CLUSTER_RESET_HARD = "CLUSTER RESET HARD"
    CLUSTER_FAILOVER = "CLUSTER FAILOVER"
    CLUSTER_SETSLOT = "CLUSTER SETSLOT"
    CLUSTER_GETKEYSINSLOT = "CLUSTER GETKEYSINSLOT"
    MIGRATE = "MIGRATE"
    
    # Command
    @staticmethod
    def cluster_meet(r, host, port):
        return r.execute_command(RedisUtils.CLUSTER_MEET, host, port)
    
    @staticmethod
    def cluster_add_slots(r, slots):
        return r.execute_command(RedisUtils.CLUSTER_ADDSLOTS, *slots)
    
    @staticmethod
    def cluster_myid(r):
        return r.execute_command(RedisUtils.CLUSTER_MYID)
    
    @staticmethod
    def cluster_replica(r, node_id):
        return r.execute_command(RedisUtils.CLUSTER_REPLICATE, node_id)
    
    @staticmethod
    def cluster_nodes(r):
        return r.execute_command(RedisUtils.CLUSTER_NODES)
    
    @staticmethod
    def cluster_forget(r, node_id):
        return r.execute_command(RedisUtils.CLUSTER_FORGET, node_id)

    @staticmethod
    def cluster_reset(r):
        return r.execute_command(RedisUtils.CLUSTER_RESET)

    @staticmethod
    def cluster_reset_hard(r):
        return r.execute_command(RedisUtils.CLUSTER_RESET_HARD)

    @staticmethod
    def manual_failover(r):
        return r.execute_command(RedisUtils.CLUSTER_FAILOVER)


    # reshard - slot migration
    @staticmethod
    def set_slot_importing(to_conn, slot, from_node_id):
        """
        타겟 노드에서 해당 슬롯을 IMPORTING 상태로 설정
        """
        to_conn.execute_command(RedisUtils.CLUSTER_SETSLOT, slot, "IMPORTING", from_node_id)

    @staticmethod
    def set_slot_migrating(from_conn, slot, to_node_id):
        """
        소스 노드에서 해당 슬롯을 MIGRATING 상태로 설정
        """
        from_conn.execute_command(RedisUtils.CLUSTER_SETSLOT, slot, "MIGRATING", to_node_id)

    @staticmethod
    def set_slot_node(conn, slot, node_id):
        """
        해당 노드에서 슬롯의 최종 소유자를 지정 (CLUSTER SETSLOT <slot> NODE <node_id>)
        슬롯 이동이 끝난 후 MIGRATING/IMPORTING 상태를 NODE 상태로 덮어씌워 소유권 확정
        """
        conn.execute_command(RedisUtils.CLUSTER_SETSLOT, slot, "NODE", node_id)

    @staticmethod
    def get_keys_in_slot(conn, slot, count):
        """
        주어진 슬롯(slot)에서 최대 count 개수만큼의 키를 조회
        """
        return conn.execute_command(RedisUtils.CLUSTER_GETKEYSINSLOT, slot, count)
    
    @staticmethod
    def migrate_key(from_conn, to_host, to_port, key, password, timeout=60000): 
        """
        특정 키를 대상 Redis로 MIGRATE
        - 요구사항에 근거하여 timeout 60000
        """
        migrate_cmd = [
            RedisUtils.MIGRATE,
            to_host,
            to_port,
            key,
            0, # db
            timeout
        ]
        if password:
            migrate_cmd += ["AUTH", password]
        from_conn.execute_command(*migrate_cmd)

    @staticmethod
    def force_failover(conn):
        """
        해당 Redis 노드에서 강제로 FAILOVER 수행
        """
        try:
            return conn.execute_command('CLUSTER', 'FAILOVER', 'FORCE')
        except Exception as e:
            print(f"❌ FAILOVER 실패: {e}")
            return None
    
    @staticmethod
    def is_replica(redis_conn):
        info = redis_conn.info('replication')
        return info.get('role') == 'slave' or info.get('role') == 'replica'


    # redis 연결 객체 생성
    @staticmethod   
    def create_redis_with_pool(host, port, password):
        pool = redis.ConnectionPool(host=host, port=port, password=password, decode_responses=True)
        return redis.Redis(connection_pool=pool)
    
    @staticmethod
    def connect_node(host, port, password):
        """
        주어진 호스트, 포트, 비밀번호를 사용하여 Redis 노드에 연결 + Redis 인스턴스(= Redis Client) 반환
        """
        r = redis.Redis(
            host=host,
            port=port,
            password=password,
            decode_responses=True  # Redis에서 조회한 문자열을 bytes가 아닌 str로 반환
        )
        
        try:
            r.ping()
        except redis.exceptions.RedisError as e:
            print(f"❌ Redis 연결 실패: {e}")
            sys.exit(1)
        
        return r

    
    @staticmethod
    def connect_redis_cluster(host, port, password):
        return redis.RedisCluster(
            host=host,
            port=port,
            password=password,
            decode_responses=True,
            skip_full_coverage_check=True,
        )
    
    @staticmethod
    def get_cluster_nodes(connection):
        """
        주어진 임의의 연결 노드를 통해 CLUSTER_NODE 명령어 호출하여 모든 노드 정보 추출
        """
        try:
            return RedisUtils.cluster_nodes(connection)
        except redis.exceptions.RedisError as e:
            PrintUtils.error(f"CLUSTER NODES 명령 실행 실패: {e}\n")
            sys.exit(1)
        
    
    # 버전 체크
    @staticmethod
    def redis_version():
        """
        현재 설치된 redis-py 라이브러리의 버전을 출력합니다.
        """
        print(redis.__version__)