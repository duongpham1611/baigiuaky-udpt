import subprocess
import time
import sys
import os

# Path to python executable
PYTHON = sys.executable

def main():
    print("Starting 3 nodes...")
    
    # Needs absolute path to node.py and config.json
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Assuming run_cluster.py is in src/, otherwise adjust
    # If run_cluster.py is in e:/GiuakiUDPT, then node is at src/node.py
    # But I will put run_cluster.py in src/ as well for simplicity with imports?
    # No, usually run scripts are at root. Let's assume root.
    
    # Actually, I'll write this file to root e:/GiuakiUDPT/run_cluster.py
    
    node_script = os.path.join("src", "node.py")
    config_file = os.path.join("src", "config.json") # or just absolute path
    config_file_abs = os.path.abspath(os.path.join("src", "config.json"))

    processes = []
    
    for node_id in ["node1", "node2", "node3"]:
        print(f"Launching {node_id}...")
        cmd = [PYTHON, node_script, node_id, config_file_abs]
        # On Windows, creationflags=subprocess.CREATE_NEW_CONSOLE opens new window
        # which is nice for viewing logs.
        p = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
        processes.append(p)
        time.sleep(1)

    print("Cluster running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
            # Check if processes are alive
            for p in processes:
                if p.poll() is not None:
                   print("One node died!")
    except KeyboardInterrupt:
        print("Stopping cluster...")
        for p in processes:
            p.terminate()

if __name__ == "__main__":
    main()
