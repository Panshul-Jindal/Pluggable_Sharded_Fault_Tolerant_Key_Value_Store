import os
import shutil
import subprocess
import json
import matplotlib.pyplot as plt
import sys
import glob

# === CONFIGURATION ===
RAFT_DIR = "raft1"      
ZK_DIR = "zookeeper"    

TARGET_FILE = os.path.join(RAFT_DIR, "raft.go")
BACKUP_FILE = os.path.join(RAFT_DIR, "raft.go.bak")

RAFT_SOURCE = TARGET_FILE
ZK_SOURCE = os.path.join(ZK_DIR, "zookeeper.go")

def preflight_check():
    print("--- Pre-flight Check ---")
    if not os.path.exists(RAFT_DIR):
        print(f"CRITICAL: Directory {RAFT_DIR} not found.")
        return False
    
    # Check for package conflict
    go_files = glob.glob(os.path.join(RAFT_DIR, "*.go"))
    packages = set()
    for f in go_files:
        with open(f, 'r', errors='ignore') as file:
            for line in file:
                if line.startswith("package "):
                    packages.add(line.strip())
                    break
    
    if len(packages) > 1:
        print(f"WARNING: Multiple packages found in {RAFT_DIR}: {packages}")
        print("Go does not allow mixed packages in one directory. This will cause build failures.")
        print("Please delete files belonging to the wrong package.")
    
    return True

def run_test(algo_name):
    print(f"\n--- Running {algo_name} ---")
    cmd = ["go", "test", "-v", "-run", "TestPerf"]
    
    try:
        # Capture BOTH stdout and stderr
        result = subprocess.run(cmd, cwd=RAFT_DIR, capture_output=True, text=True, timeout=120)
        
        metrics = []
        for line in result.stdout.splitlines():
            if line.startswith("BENCH_DATA:"):
                json_str = line.replace("BENCH_DATA:", "")
                data = json.loads(json_str)
                data['algorithm'] = algo_name
                metrics.append(data)
                print(f"  -> {data['test_name']}: {data['value']:.2f} {data['unit']}")
        
        if result.returncode != 0:
            print(f"  -> {algo_name} Failed! Printing errors:")
            print("----------------- STDERR -----------------")
            print(result.stderr) # Print the actual error log
            print("----------------- STDOUT (Tail) -----------------")
            print("\n".join(result.stdout.splitlines()[-10:]))
            
        return metrics
    except subprocess.TimeoutExpired:
        print("  -> Timeout!")
        return []
    except Exception as e:
        print(f"  -> Error: {e}")
        return []

def plot_results(metrics):
    tests = list(set(m['test_name'] for m in metrics))
    for t in tests:
        raft_data = next((m for m in metrics if m['test_name'] == t and m['algorithm'] == 'Raft'), None)
        zk_data = next((m for m in metrics if m['test_name'] == t and m['algorithm'] == 'Zookeeper'), None)
        
        if not raft_data or not zk_data: continue

        plt.figure(figsize=(6, 4))
        plt.bar(['Raft', 'Zookeeper'], [raft_data['value'], zk_data['value']], color=['#3498db', '#e74c3c'])
        plt.title(f"{t} Comparison")
        plt.ylabel(raft_data['unit'])
        plt.grid(axis='y', linestyle='--', alpha=0.5)
        
        fname = f"bench_{t.replace(' ', '_')}.png"
        plt.savefig(fname)
        print(f"Generated plot: {fname}")
        plt.close()

def main():
    if not preflight_check(): return
    
    if os.path.exists(TARGET_FILE):
        shutil.copy(TARGET_FILE, BACKUP_FILE)

    try:
        all_metrics = []
        all_metrics.extend(run_test("Raft"))
        
        if os.path.exists(ZK_SOURCE):
            shutil.copy(ZK_SOURCE, TARGET_FILE)
            all_metrics.extend(run_test("Zookeeper"))
        else:
            print(f"Skipping Zookeeper: {ZK_SOURCE} not found")
        
        if all_metrics: plot_results(all_metrics)
        else: print("\nNo metrics collected.")

    finally:
        if os.path.exists(BACKUP_FILE):
            shutil.move(BACKUP_FILE, TARGET_FILE)
            print("\nRestored original raft.go")

if __name__ == "__main__":
    main()