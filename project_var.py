import sys, os

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
LOGGING_DIR = os.path.join(PROJECT_DIR, "logs")
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
if not os.path.exists(LOGGING_DIR):
    os.makedirs(LOGGING_DIR)