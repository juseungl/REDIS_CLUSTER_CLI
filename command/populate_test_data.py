import sys
from tqdm import tqdm
from utils.string_utils import StringUtils
from utils.print_utils import PrintUtils
from utils.redis_utils import RedisUtils


def populate_test_data(node_addr, password, num_keys=1000):
    # 요구사항 따라 생성할 키 수 검증
    validate_key_count(num_keys)
    # 레디스 클러스터에 접속
    r = connect_to_cluster(node_addr, password)
    # 더미 데이터 생성
    generate_dummy_data_no_batch(r, num_keys)



def validate_key_count(num_keys):
    """
    입력된 키 개수가 1 이상 10,000,000 이하인지 검증.
    벗어나면 프로그램 종료.
    """
    if num_keys < 1 or num_keys > 10_000_000:
        print("❌ 요구사항에 따라 --num-of-keys 값은 1 이상 10,000,000 이하만 가능합니다.")
        sys.exit(1)


def connect_to_cluster(node_addr, password):
    """
    지정된 노드 주소로 Redis Cluster에 연결 시도.
    연결 실패 시 프로그램 종료.
    """
    host, port = StringUtils.parse_node(node_addr)
    print(f"🔍 {node_addr}가 포함 된 클러스터에 연결 중...")

    try:
        r = RedisUtils.connect_redis_cluster(host, port, password)
        r.ping()
        return r
    except Exception as e:
        print(f"❌ 클러스터 연결 실패: {e}")
        sys.exit(1)


def generate_dummy_data_no_batch(r, num_keys):
    print(f"⏳ 총 {num_keys:,} 개의 더미 데이터(string 키-값)을 생성합니다...")

    for i in tqdm(range(1, num_keys+1), desc="📦 Redis에 저장 중", unit="key"):
        key = f"key:{i:010d}"
        val = f"val:{i:010d}"
        try:
            r.set(key, val)
        except Exception as e:
            print(f"\n⚠️ 에러 발생 (key: {key}): {e}")

    print(f"\n🎉 더미 데이터 생성 완료! 총 {num_keys}개 키가 저장되었습니다.")
