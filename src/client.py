import grpc
import sys
import os
import argparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import service_pb2
import service_pb2_grpc

def run_client(host, port, command, key, value=None):
    with grpc.insecure_channel(f'{host}:{port}') as channel:
        stub = service_pb2_grpc.KVServiceStub(channel)
        
        try:
            if command == 'put':
                if value is None:
                    print("Error: Value required for put")
                    return
                response = stub.Put(service_pb2.PutRequest(key=key, value=value))
                print(f"PUT Response: {response.message} (Success: {response.success})")
                
            elif command == 'get':
                response = stub.Get(service_pb2.GetRequest(key=key))
                if response.found:
                    print(f"GET Response: Found value '{response.value}'")
                else:
                    print("GET Response: Key not found")
                    
            elif command == 'delete':
                response = stub.Delete(service_pb2.DeleteRequest(key=key))
                print(f"DELETE Response: {response.message} (Success: {response.success})")
            else:
                print(f"Unknown command: {command}")
                
        except grpc.RpcError as e:
            print(f"RPC Error: {e.code()} - {e.details()}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Distributed KV Client")
    parser.add_argument("--host", default="localhost", help="Node Host")
    parser.add_argument("--port", type=int, default=50051, help="Node Port")
    parser.add_argument("command", choices=["put", "get", "delete"], help="Command")
    parser.add_argument("key", help="Key")
    parser.add_argument("value", nargs="?", help="Value (for put)")

    args = parser.parse_args()
    run_client(args.host, args.port, args.command, args.key, args.value)
