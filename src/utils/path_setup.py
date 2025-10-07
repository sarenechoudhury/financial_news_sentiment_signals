import os
import sys

# Path to project root (go one level up from src/)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

print(f"[Path setup] Added project root to sys.path: {ROOT_DIR}")