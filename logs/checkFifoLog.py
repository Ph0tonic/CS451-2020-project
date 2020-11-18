import sys

HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKCYAN = '\033[96m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'

if len(sys.argv) != 3:
	print("Usage: checkFifoLog.py <n_messages> <n_processes>")
	sys.exit()
	
try:
	n = int(sys.argv[1])
	n_proc = int(sys.argv[2])
except:
	print("Usage: checkFifoLog.py <n_messages> <n_processes>")

for log_id in range(n_proc):
	
	print(f"---------------------------------\nChecking logs of process {log_id + 1}")
	
	f = open(f"proc{log_id+1:02}.output")
	logs = [x.strip() for x in f]
	if(len(logs) == 0):
		print(f"Process {log_id + 1} has no output (was probably killed)")
		continue
	
	# Check all broadcasts are present
	for x in range(n):
		if f"b {x+1}" not in logs:
			print(f"[{WARNING}!!{ENDC}]b {x+1} not in logs !")
	print(f"[{OKGREEN}OK{ENDC}] All broadcasts are present")
	
	# Check all reliveries are present
	for proc_id in range(n_proc):
		all_deliveries_present = True
		for x in range(n):
			if f"d {proc_id + 1} {x+1}" not in logs:
				print(f"[{WARNING}!!{ENDC}] d {proc_id + 1} {x+1} not in logs !")
				all_deliveries_present = False
		if all_deliveries_present:
			print(f"[{OKGREEN}OK{ENDC}] All deliveries are present for process {proc_id + 1}")
		else:
			print(f"[{WARNING}!!{ENDC}] Not all deliveries are present for process {proc_id + 1}")
	
	# Check there are not too many logs
	if(len(logs) > n_proc*n):
		print(f"[{WARNING}!!{ENDC}] Too many logs ! {len(logs)} logs instead of {3*n}.")
	else:
		print(f"[{OKGREEN}OK{ENDC}] Not too many logs")
		
	# Check broadcasts are in order
	broadcasts = [x for x in logs if "b" in x]
	last_broadcast = 0
	b_are_in_order = True
	for b in broadcasts:
		if(str(last_broadcast + 1) not in b):
			print(f"[{WARNING}!!{ENDC}] b {last_broadcast + 1} does not come after b {last_broadcast} !")
			b_are_in_order = False
		last_broadcast = last_broadcast + 1
		
	if(b_are_in_order):
		print(f"[{OKGREEN}OK{ENDC}] Broadcasts are in order")
	else:
		print(f"[{WARNING}!!{ENDC}] Broadcasts are NOT in order")
		
	# Check deliveries are in order
	for proc_id in range(n_proc):
		deliveries = [x for x in logs if f"d {proc_id + 1} " in x]
		last_delivery = 0
		d_are_in_order = True
		for d in deliveries:
			if(str(last_delivery + 1) not in d):
				print(f"[{WARNING}!!{ENDC}] d {proc_id + 1} {last_delivery + 1} does not come after d {proc_id + 1} {last_delivery} !")
				d_are_in_order = False
			last_delivery = last_delivery + 1
			
		if(d_are_in_order):
			print(f"[{OKGREEN}OK{ENDC}] Deliveries of messages from {proc_id + 1} are in order")
		else:
			print(f"[{WARNING}!!{ENDC}] Deliveries of messages from {proc_id + 1} are NOT in order")
		

