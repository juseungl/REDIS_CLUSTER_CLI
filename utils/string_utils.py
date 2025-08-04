import sys
import redis

class StringUtils:
    @staticmethod
    def parse_node(host_port):
        """
        주어진 ip:port를 파싱하여 IP 주소와 포트 번호를 분리하여 반환.
        example: host_port = "127.0.0.1:9000" -> ("127.0.0.1", 9000)]
        """
        try:
            host, port = host_port.split(":")
            return host, int(port)
        except Exception:
            print(f"잘못된 노드 주소 형식입니다: {host_port} (형식: ip:port)")
            sys.exit(1)
