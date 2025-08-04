import argparse
import sys

from command import *

# 서브 커맨드 핸들러 import (아직 미구현 시, 임시 패스)
# from commands import create, add_node, reshard, del_node, check, populate, help_cmd

def subcommand_handler():
    parser = argparse.ArgumentParser(
        usage='./rcctl [--user USER] --password PASSWORD <subcommand> [options]',
        description=(
            "[ Redis Cluster CLI - 레디스 클러스터 관리 CLI 툴 ]\n\n"
            "클러스터 생성, 노드 추가/삭제, 슬롯 리샤딩, 상태 점검, 테스트 데이터 삽입 등\n"
            "다양한 Redis Cluster 관리 기능을 제공합니다.\n\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )


    # 공통 옵션 (global)
    parser.add_argument("--user", type=str, default="default", help="Redis 사용자 이름 (기본: default)")
    parser.add_argument("--password", type=str, help="Redis 노드 비밀번호")

    # 서브 커맨드 파서
    subparsers = parser.add_subparsers(dest="command", help="서브 커맨드 목록")     
    
    # help
    help_parser = subparsers.add_parser("help", help="사용법 안내")

    # create
    create_parser = subparsers.add_parser("create", help="Redis 클러스터 생성")
    create_parser.add_argument("--replicas", type=int, default=0, help="각 마스터 당 리플리카 수 (기본: 0)")
    create_parser.add_argument("nodes", nargs='+', help="클러스터에 사용할 노드들 (ip:port 형식)") # nargs필드는 한 개 이상의 인자를 받을 수 있음.

    # add-node
    add_node_parser = subparsers.add_parser("add-node", help="노드 추가")
    add_node_parser.add_argument("--master-id", type=str, help="리플리카일 경우, 연결할 마스터의 ID")
    add_node_parser.add_argument("new_node", help="추가할 노드 (ip:port)")
    add_node_parser.add_argument("existing_node", help="기존 클러스터 노드 (ip:port)")

    # reshard
    reshard_parser = subparsers.add_parser("reshard", help="슬롯 리샤딩")
    reshard_parser.add_argument("--from", dest="from_node", required=True, help="슬롯 이동할 원본 노드 ID")
    reshard_parser.add_argument("--to", dest="to_node", required=True, help="슬롯이 이동될 대상 노드 ID")
    reshard_parser.add_argument("--slots", type=int, required=True, help="이동할 슬롯 개수")
    reshard_parser.add_argument("--pipeline", type=int, default=10, help="한 번에 이동할 키 수 (기본: 10)")
    reshard_parser.add_argument("target_node", help="명령 실행을 위한 클러스터 노드 (ip:port)")

    # del-node
    del_node_parser = subparsers.add_parser("del-node", help="노드 제거")
    del_node_parser.add_argument("target_node", help="클러스터 노드 (ip:port)")
    del_node_parser.add_argument("node_id", help="제거할 노드 ID")

    # check
    check_parser = subparsers.add_parser("check", help="클러스터 상태 확인")
    check_parser.add_argument("target_node", help="클러스터 노드 (ip:port)")

    # populate-test-data
    populate_parser = subparsers.add_parser("populate-test-data", help="테스트 데이터 생성")
    populate_parser.add_argument("--num-of-keys", type=int, default=1000, help="생성할 키 수 (기본: 1000)")
    populate_parser.add_argument("node_addr", help="Redis 노드 주소 (ip:port)")



    # 파싱 및 실행
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # help 명령어일 경우 password 검사 스킵
    if args.command != "help" and not args.password:
        print("\n\n❗ 오류: --password 옵션은 필수입니다.\n")
        parser.print_help()
        sys.exit(1)



    # 서브커맨드 매핑
    if args.command == "help":
        parser.print_help()
    elif args.command == "create":
        create(args.nodes, args.replicas, args.password)
    elif args.command == "add-node":
        add_node(args.new_node, args.existing_node, args.password, args.master_id)
    elif args.command == "reshard":
        reshard(args.from_node, args.to_node, args.slots, args.pipeline, args.target_node, args.password)
    elif args.command == "del-node":
        del_node(args.target_node, args.node_id, args.password)
    elif args.command == "check":
        check(args.target_node, args.password)
    elif args.command == "populate-test-data":
        populate_test_data(args.node_addr, args.password, args.num_of_keys)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)