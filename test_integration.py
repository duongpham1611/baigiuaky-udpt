import subprocess
import time
import sys
import os
import grpc
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
import service_pb2
import service_pb2_grpc

PYTHON = sys.executable

class TestKVStore(unittest.TestCase):
    processes = []

    @classmethod
    def setUpClass(cls):
        print("Starting cluster for testing...")
        node_script = os.path.join("src", "node.py")
        config_file = os.path.abspath(os.path.join("src", "config.json"))
        
        for node_id in ["node1", "node2", "node3"]:
            cmd = [PYTHON, node_script, node_id, config_file]
            # Enable output for debugging
            p = subprocess.Popen(cmd)
            cls.processes.append(p)
        
        time.sleep(5) # Wait for startup

    @classmethod
    def tearDownClass(cls):
        print("Stopping cluster...")
        for p in cls.processes:
            p.terminate()
            p.wait()

    def get_stub(self, port):
        channel = grpc.insecure_channel(f'localhost:{port}')
        return service_pb2_grpc.KVServiceStub(channel)

    def test_basic_put_get(self):
        print("\n--- Test Basic PUT/GET ---")
        stub1 = self.get_stub(50051)
        
        # KEY1 hashes to... let's find out. 
        # But we just send to node1 (50051) and it should forward if needed.
        
        # Put
        resp = stub1.Put(service_pb2.PutRequest(key="key1", value="val1"))
        self.assertTrue(resp.success)
        
        # Get from same node
        resp = stub1.Get(service_pb2.GetRequest(key="key1"))
        self.assertTrue(resp.found)
        self.assertEqual(resp.value, "val1")
        
        # Get from another node (should forward or have replica)
        stub2 = self.get_stub(50052)
        resp = stub2.Get(service_pb2.GetRequest(key="key1"))
        if resp.found:
             self.assertEqual(resp.value, "val1")
        else:
             # If expected to forward, it should have found it.
             self.fail("Node 2 did not return key1 (forwarding or replication failed)")

    def test_replication_and_failure(self):
        print("\n--- Test Replication & Failure ---")
        stub1 = self.get_stub(50051)
        stub2 = self.get_stub(50052)
        stub3 = self.get_stub(50053)

        # 1. Put data to Node 1 (key='fail_test')
        # Manual Placement: Node 1 IS Primary.
        print("Putting 'fail_test' to Node 1 (Primary)")
        stub1.Put(service_pb2.PutRequest(key="fail_test", value="persist"))
        time.sleep(2) # Wait for replication
        
        # 2. Check if Node 2 has it (Replica)
        # Primary = Node 1 (50051)
        # Replica = Node 2 (50052)
        
        print("Primary is Node 1. Replica should be Node 2.")

        # Kill Primary (Node 1)
        print("Killing Primary Node 1")
        self.processes[0].terminate()
        self.processes[0].wait()
        
        time.sleep(2)
        
        # 3. Request from Replica (Node 2)
        print("Reading from Replica Node 2")
        stub_replica = self.get_stub(50052)
        
        try:
            resp = stub_replica.Get(service_pb2.GetRequest(key="fail_test"))
            self.assertTrue(resp.found)
            self.assertEqual(resp.value, "persist")
            print("Success: Replica served the data!")
        except Exception as e:
            self.fail(f"Replica failed to serve data: {e}")
            
        # Restart Primary (Node 1)
        print("Restarting Primary...")
        node_script = os.path.join("src", "node.py")
        config_file = os.path.abspath(os.path.join("src", "config.json"))
        node_id = "node1"
        cmd = [PYTHON, node_script, node_id, config_file]
        # Use stdout/stderr=None to see output or DEVNULL to hide
        new_p = subprocess.Popen(cmd) 
        self.processes[0] = new_p # Replace handle
        
        time.sleep(5)


if __name__ == '__main__':
    unittest.main()
