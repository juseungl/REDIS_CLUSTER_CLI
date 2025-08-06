class PrintUtils:
    @staticmethod
    def print_nodes_info(nodes, label):
        total = len(nodes)
        print(f"  {label}로 지정될 노드 (총 {total}개):")
        for idx, node in enumerate(nodes, start=1):
            pool = node.connection_pool
            connection = pool.get_connection('PING')  # 임시 커넥션 객체 획득
            host = connection.host
            port = connection.port
            print(f"    - {idx}/{total}. Redis node at {host}:{port}")
            pool.release(connection)  # 커넥션 반환

    @staticmethod
    def node_str(node):
        host = node.connection_pool.connection_kwargs["host"]
        port = node.connection_pool.connection_kwargs["port"]
        return f"{host}:{port}"




    @staticmethod
    def info(msg): print(f"🔍 {msg}")
    @staticmethod
    def step(msg): print(f"🗑️ {msg}")
    @staticmethod
    def success(msg): print(f"✅ {msg}")
    @staticmethod
    def warn(msg): print(f"⚠️ {msg}")
    @staticmethod
    def error(msg): print(f"❌ {msg}")
    @staticmethod
    def transition(msg): print(f"🔁 {msg}")