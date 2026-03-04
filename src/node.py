import grpc
from concurrent import futures
import time
import sys
import json
import hashlib
import threading
import socket
import os

# Add current directory to path to find generated protos
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import service_pb2
import service_pb2_grpc

class KVNode(service_pb2_grpc.KVServiceServicer, service_pb2_grpc.InternalServiceServicer):
    def __init__(self, node_id, config_path):
        self.node_id = node_id
        self.store = {} # key -> value
        self.ownership = {} # key -> owner_id (In-Memory Metadata)
        self.peers = {} # id -> stub

        self.config = self.load_config(config_path)
        self.host = ""
        self.port = 0
        self.alive_peers = set()
        self.data_file = f"{self.node_id}_data.json"
        
        # Load existing data if any
        self.load_data()

        # Parse config and setup self and peers
        self.setup_node()
        
        # Sync from peers if empty or just on startup to be safe?
        # Requirement: "node này cần yêu cầu dữ liệu thiếu từ một node khác"
        # Let's do a simple sync from all peers
        self.sync_data()

        
        # Start heartbeat thread
        self.running = True
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

    def load_data(self):
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    for k, v in data.items():
                        self.store[k] = v
                        self.ownership[k] = self.node_id # Anything on disk is MINE
                print(f"[{self.node_id}] Loaded {len(self.store)} keys from disk.")

            except Exception as e:
                print(f"[{self.node_id}] Failed to load data: {e}")

    def save_data(self):
        try:
            # SAVE EVERYTHING (Primary + Replica)
            # User wants persistence in file. "Clean" format means just k:v.
            # We already have self.store as k:v.
            with open(self.data_file, 'w') as f:
                json.dump(self.store, f)
        except Exception as e:
            print(f"[{self.node_id}] Failed to save data: {e}")



    def get_prev_node_id(self):
        sorted_nodes = sorted(self.config, key=lambda x: x['id'])
        my_idx = -1
        for i, node in enumerate(sorted_nodes):
            if node['id'] == self.node_id:
                my_idx = i
                break
        if my_idx == -1: return None
        prev_idx = (my_idx - 1) % len(sorted_nodes)
        return sorted_nodes[prev_idx]['id']

    def sync_data(self):
        print(f"[{self.node_id}] Starting sync...")
        prev_id = self.get_prev_node_id()
        
        for node in self.config:
            if node['id'] == self.node_id: continue
            
            target_id = node['id']
            try:
                channel = grpc.insecure_channel(f"{node['host']}:{node['port']}")
                stub = service_pb2_grpc.InternalServiceStub(channel)
                
                resp = stub.RequestSnapshot(service_pb2.SnapshotRequest(node_id=self.node_id), timeout=2)
                
                count = 0
                for k, v in resp.data.items():
                    # If from Prev Node -> It's likely his Primary -> I am Replica.
                    if target_id == prev_id:
                        self.store[k] = v
                        self.ownership[k] = target_id
                        count += 1
                
                if count > 0:
                    print(f"[{self.node_id}] Synced {count} items from {target_id}")

            except Exception as e:
                pass
        
        self.save_data()




    def load_config(self, path):
        with open(path, 'r') as f:
            return json.load(f)

    def setup_node(self):
        for node in self.config:
            if node['id'] == self.node_id:
                self.host = node['host']
                self.port = node['port']
            else:
                # We will connect to peers lazily or periodically
                channel = grpc.insecure_channel(f"{node['host']}:{node['port']}")
                stub = service_pb2_grpc.InternalServiceStub(channel)
                self.peers[node['id']] = stub
                self.alive_peers.add(node['id'])

    # Removed Hashing/Partitioning Logics



    # --- KVService Methods ---

    def Put(self, request, context):
        key = request.key
        value = request.value
        
        # Manual Placement: I AM PRIMARY because client called me.
        print(f"[{self.node_id}] Processing PUT {key} (Primary)")
        
        # Wrap value? NO. User said "clean value".
        # We store metadata in self.ownership
        
        # Unique Key Check: Scan network first.
        # This is expensive but ensures uniqueness.
        check_resp = self.Get(service_pb2.GetRequest(key=key), None)
        if check_resp.found:
             # If found, check if value differs
             # Actually requirement is "Keys không được trùng nhau" -> Exists = Error.
             # But wait, what if I want to update? 
             # Usually PUT = Insert or Update.
             # If User wants "Unique Key" like "Create Only", that's different.
             # User said: "các key không được trùng nhau". 
             # Assuming this means "Cannot overwrite existing key from another node" or "Cannot introduce duplicate".
             # If key exists, we should probably Reject or Update?
             # "Key không được trùng nhau" implies unique constraint.
             # If it exists, we reject.
             print(f"[{self.node_id}] Key {key} already exists. Rejecting.")
             return service_pb2.PutResponse(success=False, message="Key already exists")

        self.store[key] = value

        self.ownership[key] = self.node_id
        
        self.save_data()
        
        # Replicate to next node (Ring)
        self.replicate_data(key, value, "PUT")

        
        return service_pb2.PutResponse(success=True, message="Key stored")


    def Get(self, request, context):
        key = request.key
        
        # 1. Check Local
        if key in self.store:
            # No Unwrap needed. Raw value.
            print(f"[{self.node_id}] Serving GET {key} from local")
            return service_pb2.GetResponse(found=True, value=self.store[key])

            
        # 2. If missing, Broadcast/Forward to Neighbors
        # A simple approach: Ask all peers.
        print(f"[{self.node_id}] Key {key} not found locally, asking peers...")
        
        for node in self.config:
            if node['id'] == self.node_id: continue
            
            # Avoid infinite loops? We assume client asks Node X. Node X asks Y and Z. 
            # If Y asks X again? 
            # We need to distinguish "Client Request" vs "Internal Forward".
            # But the simplest is: Node X asks Y. Y checks local. Y returns.
            # IMPORTANT: Y should NOT forward again.
            # How to tell Y? 
            # Using KVService.Get implies "Do whatever". 
            # But if we assume 1 hop is enough (data is allocated somewhere), we can just try.
            
            try:
                # Connect
                channel = grpc.insecure_channel(f"{node['host']}:{node['port']}")
                stub = service_pb2_grpc.InternalServiceStub(channel)
                
                # We reuse InternalService for "Query" or just use RequestSnapshot? No.
                # Let's use KVService but we need to prevent loop.
                # Actually, RequestSnapshot gets ALL data. We could use that? A bit heavy.
                # Let's add `InternalGet`? Or just trust that we only call KVService.Get from Client.
                # If I call KVService.Get on Peer, Peer will check local, fail, and then... ask Peers (including me). LOOP.
                
                # SOLUTION: Check peers using `RequestSnapshot`? No.
                # SOLUTION: Just check using `InternalService.Replicate` (hack)? No.
                # SOLUTION: Add a simple internal method or just assume if client connects to correct node.
                # But requirement says "node phải chuyển tiếp".
                
                # Let's perform a "One-Hop Query":
                # But Peer's `Get` has logic to broadcast.
                # I can't easily change Protobuf now (requires recompiling).
                # I will try to infer: If I receive a request, I check local.
                # Wait, if I change GET to: Check local -> Return.
                # Then Client logic (or Forwarding logic) handles the rest.
                # If I Forward, I want target to ONLY check local.
                # I can't signal that without proto change.
                
                # FAST FIX: Use `InternalService.RequestSnapshot` to get ALL keys from peer and check in memory here?
                # Heavy but safe loop-free.
                # Better: Check peers returns `found`?
                
                # Let's assume the graph is small (3 nodes).
                # Node 1 asks Node 2. Node 2 shouldn't ask back.
                # I'll modify GET logic: Check Local. If not found, return Not Found.
                # AND Client (or Proxy) iterates?
                # The user requirement: "node được liên hệ phải chuyển tiếp".
                # This implies Server-Side forwarding.
                
                # I will implement "Query Peers" by pulling their data map via `RequestSnapshot` (easy, existing)
                # It's inefficient but guarantees no loops and works with current Proto.
                
                snap_stub = service_pb2_grpc.InternalServiceStub(channel)
                snap = snap_stub.RequestSnapshot(service_pb2.SnapshotRequest(node_id=self.node_id))
                
                if key in snap.data:
                    # Found it!
                    real_v = snap.data[key] # No unwrap
                    return service_pb2.GetResponse(found=True, value=real_v)

            except:
                pass
                
        return service_pb2.GetResponse(found=False, value="")


    def Delete(self, request, context):
        key = request.key
        # Broadcast Delete or just Delete everywhere?
        # With manual placement, we don't know who has it.
        # So we tell EVERYONE to delete.
        print(f"[{self.node_id}] Processing DELETE {key} (Broadcasting)")
        
        # Delete local
        if key in self.store:
            del self.store[key]
            self.save_data()
        
        # Tell neighbors
        for node in self.config:
            if node['id'] == self.node_id: continue
            try:
                # Use Replicate(DELETE)
                channel = grpc.insecure_channel(f"{node['host']}:{node['port']}")
                stub = service_pb2_grpc.InternalServiceStub(channel)
                stub.Replicate(service_pb2.ReplicateRequest(key=key, value="", op="DELETE"))
            except:
                pass
        
        return service_pb2.DeleteResponse(success=True, message="Deleted")




    # --- InternalService Methods ---
    
    def Replicate(self, request, context):
        key = request.key
        print(f"[{self.node_id}] Replication request for {key} ({request.op})")
        if request.op == "PUT":
            self.store[key] = request.value
            # Owner is sender (or passed in field)
            if request.owner:
                self.ownership[key] = request.owner
            else:
                 # Infer? If unspecified, maybe we don't know.
                 pass
                 
            # I am replica. Save to disk as requested.
            self.save_data()
            pass

            
        elif request.op == "DELETE":
            if key in self.store:
                del self.store[key]
                if key in self.ownership: del self.ownership[key]
                self.save_data() # To remove if it was persisted

        return service_pb2.ReplicateResponse(success=True)


    def Heartbeat(self, request, context):
        return service_pb2.HeartbeatResponse(alive=True)

    def RequestSnapshot(self, request, context):
        # Return all data
        # Ideally this should be filtered by what the requester needs (partitions), 
        # but for simplicity, return everything (simulating full partition sync)
        return service_pb2.SnapshotResponse(data=self.store)

    # --- Helpers ---

    def replicate_data(self, key, value, op):
        # Replicate to next node in ring
        # Sort nodes, find my index i, replicate to (i+1)%n
        sorted_nodes = sorted(self.config, key=lambda x: x['id'])
        my_idx = -1
        for i, node in enumerate(sorted_nodes):
            if node['id'] == self.node_id:
                my_idx = i
                break
        
        if my_idx == -1: return

        # Replica 1
        next_idx = (my_idx + 1) % len(sorted_nodes)
        
        # Simple loop to replicate to 1 successor (Replica Factor = 2: Primary + 1 Backup)
        # Request says "at least two copies". So 1 backup is enough.
        target = sorted_nodes[next_idx]
        target_id = target['id']
        
        # Don't replicate to self
        if target_id == self.node_id: return 

        try:
            if target_id in self.peers:
                self.peers[target_id].Replicate(
                    service_pb2.ReplicateRequest(key=key, value=value, op=op, owner=self.node_id)
                )

            else:
                 # Try to connect if missing (lazy reconnect)
                 pass # simplified
        except Exception as e:
            print(f"[{self.node_id}] Failed to replicate to {target_id}: {e}")

    def heartbeat_loop(self):
        while self.running:
            time.sleep(5)
            # Ping all peers
            for node in self.config:
                if node['id'] == self.node_id: continue
                try:
                    # Re-create stub or use existing safely
                    # For simplicity, creates a fresh channel check usually?
                    # Existing stub in self.peers
                    if node['id'] in self.peers:
                        self.peers[node['id']].Heartbeat(service_pb2.HeartbeatRequest(node_id=self.node_id))
                        # print(f"[{self.node_id}] Heartbeat to {node['id']} OK")
                except Exception as e:
                    print(f"[{self.node_id}] Peer {node['id']} is unreachable.")
                    # Handle failure?
                    # If this was my replica, I might need to replicate to the NEXT one?
                    # For this exercise, simple detection is requested.


def serve(node_id, config_path):
    # Load config to get port
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    
    my_conf = next((n for n in config_data if n['id'] == node_id), None)
    if not my_conf:
        print(f"Node {node_id} not found in config")
        return

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node = KVNode(node_id, config_path)
    
    service_pb2_grpc.add_KVServiceServicer_to_server(node, server)
    service_pb2_grpc.add_InternalServiceServicer_to_server(node, server)
    
    server.add_insecure_port(f"[::]:{my_conf['port']}")
    print(f"Node {node_id} started on port {my_conf['port']}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python node.py <node_id> <config_file>")
        sys.exit(1)
    
    serve(sys.argv[1], sys.argv[2])
