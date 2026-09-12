import os
import socket
import time
import pandas as pd

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "traffic_accidents.csv")


def start_server(host="localhost", port=9999, delay=0.5):
    df = pd.read_csv(DATA_PATH)
    total = len(df)

    print("=" * 60)
    print(" TrafficWatch - TCP Stream Feed Simulator")
    print("=" * 60)
    print(f"\nRecords: {total} | Delay: {delay}s | Port: {port}\n")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(1)
        print(f"✓ Server on {host}:{port} - waiting for client...")
        conn, addr = s.accept()
        print(f"✓ Client connected: {addr}\n")
        with conn:
            for i, (_, row) in enumerate(df.iterrows(), start=1):
                msg = row.to_json() + "\n"
                try:
                    conn.sendall(msg.encode("utf-8"))
                except (BrokenPipeError, ConnectionResetError):
                    print("Client disconnected.")
                    break
                if i % 20 == 0 or i == total:
                    print(f"  [{i}/{total}] region={row.get('region', '?')}")
                time.sleep(delay)


if __name__ == "__main__":
    start_server() 
