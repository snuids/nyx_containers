import sys
import json

print("Starting Report...")

report=json.loads(sys.argv[1])

# WRITE a txt file that contains the input parameters
with open(report["output"]+".txt", "w+") as text_file:
    text_file.write(json.dumps(report))
