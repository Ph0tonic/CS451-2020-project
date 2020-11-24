#!/usr/bin/env python3

import argparse
import os, atexit
import textwrap
import time
import tempfile
import threading, subprocess
import barrier, finishedSignal


import signal
import random
import time
from enum import Enum

from collections import defaultdict, OrderedDict


BARRIER_IP = 'localhost'
BARRIER_PORT = 10000

SIGNAL_IP = 'localhost'
SIGNAL_PORT = 11000

PROCESSES_BASE_IP = 11000


# Additionals methods for validation
def vec_leq(vec1, vec2):
  """ Check that vec1 is less than vec2 elemtwise """
  assert isinstance(vec1, list), "Only work with lists"
  assert isinstance(vec2, list), "Only work with lists"
  assert len(vec1) == len(vec2), "Vector lengths should be the same"
  for x, y in zip(vec1, vec2):
    # if a coordinate is greater, returning false
    if x > y: return False

  # returning True if all xs are <= than ys
  return True

# small sanity check before starting the actual test
assert vec_leq([1,1,1], [1,1,1]) == True
assert vec_leq([1,0,1], [1,1,1]) == True
assert vec_leq([1,0,1], [0,1,1]) == False
assert vec_leq([0,0,1], [1,0,0]) == False

def soft_assert(condition, message = None):
    """ Print message if there was an error without exiting """
    global were_errors
    if not condition:
        if message:
            print("ASSERT failed " + message)
        were_errors = True

# Do not run multiple validations concurrently!
class TC:
    def __init__(self, losses, interface="lo", needSudo=True, sudoPassword="dcl"):
        self.losses = losses
        self.interface = interface
        self.needSudo = needSudo
        self.sudoPassword = sudoPassword

        cmd1 = 'tc qdisc add dev {} root netem 2>/dev/null'.format(self.interface)
        cmd2 = 'tc qdisc change dev {} root netem delay {} {} loss {} {} reorder {} {}'.format(self.interface, *self.losses['delay'], *self.losses['loss'], *self.losses['reordering'])

        if self.needSudo:
            os.system("echo {} | sudo -S {}".format(self.sudoPassword, cmd1))
            os.system("echo {} | sudo -S {}".format(self.sudoPassword, cmd2))
        else:
            os.system(cmd1)
            os.system(cmd2)

        atexit.register(self.cleanup)

    def __str__(self):
        ret = """\
        Interface: {}
          Delay: {} {}
          Loss: {} {}
          Reordering: {} {}""".format(
              self.interface,
              *self.losses['delay'],
              *self.losses['loss'],
              *self.losses['reordering'])

        return textwrap.dedent(ret)

    def cleanup(self):
        cmd = 'tc qdisc del dev {} root 2>/dev/null'.format(self.interface)
        if self.needSudo:
            os.system("echo '{}' | sudo -S {}".format(self.sudoPassword, cmd))
        else:
            os.system(cmd)

class ProcessState(Enum):
    RUNNING = 1
    STOPPED = 2
    TERMINATED = 3

class ProcessInfo:
    def __init__(self, handle):
        self.lock = threading.Lock()
        self.handle = handle
        self.state = ProcessState.RUNNING

    @staticmethod
    def stateToSignal(state):
        if state == ProcessState.RUNNING:
            return signal.SIGCONT

        if state == ProcessState.STOPPED:
            return signal.SIGSTOP

        if state == ProcessState.TERMINATED:
            return signal.SIGTERM

    @staticmethod
    def stateToSignalStr(state):
        if state == ProcessState.RUNNING:
            return "SIGCONT"

        if state == ProcessState.STOPPED:
            return "SIGSTOP"

        if state == ProcessState.TERMINATED:
            return "SIGTERM"

    @staticmethod
    def validStateTransition(current, desired):
        if current == ProcessState.TERMINATED:
            return False

        if current == ProcessState.RUNNING:
            return desired == ProcessState.STOPPED or desired == ProcessState.TERMINATED

        if current == ProcessState.STOPPED:
            return desired == ProcessState.RUNNING

        return False

class AtomicSaturatedCounter:
    def __init__(self, saturation, initial=0):
        self._saturation = saturation
        self._value = initial
        self._lock = threading.Lock()

    def reserve(self):
        with self._lock:
            if self._value < self._saturation:
                self._value += 1
                return True
            else:
                return False

class Validation:
    def __init__(self, processes, messages, outputDir):
        self.processes = processes
        self.messages = messages
        self.outputDirPath = os.path.abspath(outputDir)
        if not os.path.isdir(self.outputDirPath):
            raise Exception("`{}` is not a directory".format(self.outputDirPath))

    def generateConfig(self):
        # Implement on the derived classes
        pass

    def checkProcess(self, pid):
        # Implement on the derived classes
        pass

    def checkAll(self, continueOnError=True):
        ok = True
        for pid in range(1, self.processes+1):
            ret = self.checkProcess(pid)
            if not ret:
                ok = False

            if not ret and not continueOnError:
                return False

        return ok

class FifoBroadcastValidation(Validation):
    def generateConfig(self):
        hosts = tempfile.NamedTemporaryFile(mode='w')
        config = tempfile.NamedTemporaryFile(mode='w')

        for i in range(1, self.processes + 1):
            hosts.write("{} localhost {}\n".format(i, PROCESSES_BASE_IP+i))

        hosts.flush()

        config.write("{}\n".format(self.messages))
        config.flush()

        return (hosts, config)

    def checkProcess(self, pid):
        filePath = os.path.join(self.outputDirPath, 'proc{:02d}.output'.format(pid))

        i = 1
        nextMessage = defaultdict(lambda : 1)
        filename = os.path.basename(filePath)

        with open(filePath) as f:
            for lineNumber, line in enumerate(f):
                tokens = line.split()

                # Check broadcast
                if tokens[0] == 'b':
                    msg = int(tokens[1])
                    if msg != i:
                        print("File {}, Line {}: Messages broadcast out of order. Expected message {} but broadcast message {}".format(filename, lineNumber, i, msg))
                        return False
                    i += 1

                # Check delivery
                if tokens[0] == 'd':
                    sender = int(tokens[1])
                    msg = int(tokens[2])
                    if msg != nextMessage[sender]:
                        print("File {}, Line {}: Message delivered out of order. Expected message {}, but delivered message {}".format(filename, lineNumber, nextMessage[sender], msg))
                        return False
                    else:
                        nextMessage[sender] = msg + 1

        return True

class LCausalBroadcastValidation(Validation):
    def __init__(self, processes, messages, outputDir, extraParameter):
        super().__init__(processes, messages, outputDir)
        # Use the `extraParameter` to pass any information you think is relevant

    def generateConfig(self):
        hosts = tempfile.NamedTemporaryFile(mode='w')
        config = tempfile.NamedTemporaryFile(mode='w')

        for i in range(1, self.processes + 1):
            hosts.write("{} localhost {}\n".format(i, PROCESSES_BASE_IP+i))

        hosts.flush()

        self.causals = []
        for i in range(1, self.processes + 1):
            nb = random.randint(0, self.processes-1)
            self.causals.append(random.sample([j for j in range(1, self.processes + 1) if j != i], k=nb))

        config.write("{}\n".format(self.messages))
        print("{}".format(self.messages))
        for i in range(1, self.processes + 1):
            print("{} {}".format(i, ' '.join(str(v) for v in self.causals[i-1])))
            config.write("{} {}\n".format(i, ' '.join(str(v) for v in self.causals[i-1])))
        config.flush()

        return (hosts, config)


    def checkProcess(self, pid):
        dependencies = {i + 1: x for i, x in enumerate(self.causals)}
        # Counting processes
        n = self.processes
        print('There are %d processes' % n)

        # List of all processes
        processes = list(range(1, n + 1))

        # Printing dependencies
        for proc, deps in dependencies.items():
            pass
            # print("Process %d depends on {%s} (and itself)" % (proc, ', '.join([str(x) for x in deps])))

        # reading list of crashed processes
        crashed = list(map(int, open(os.path.join(self.outputDirPath, 'crashed.log'), 'r').read().split()))

        # creating list of correct processes
        correct = [x for x in processes if x not in crashed]

        # correct / crashed
        print("Processes %s were correct and processes %s were crashed" % (correct, crashed))

        # Reading logs
        logs = {i: list(filter(lambda x : len(x) > 0, open(os.path.join(self.outputDirPath, 'proc%02d.output' % i), 'r').read().split('\n'))) for i in processes}

        # Printing how many log messages are in the dict
        for key, value in logs.items():
            pass
            # print("Process %d: %d messages" % (key, len(value)))

        # how many messages should have been sent by each process?
        expected_messages = 10

        # messages broadcast by a process. idx -> array
        broadcast_by = {i: [] for i in processes}

        # messages delivered by a process. idx -> array
        delivered_by = {i: [] for i in processes}

        # messages delivered by a process. (idx, idx) -> array
        delivered_by_from = {(i, j): [] for i in processes for j in processes}

        # events (both deliveries and broadcasts)
        events = {i: [] for i in processes}

        # Filling in broadcast_by and delivered_by
        for process in processes:
            for entry in logs[process]:
                if entry.startswith("b "):
                    content = int(entry[2:])
                    broadcast_by[process] += [content]
                    events[process] += [('b', process, content)]
                elif entry.startswith("d "):
                    by = process
                    from_ = int(entry.split()[1])
                    content = int(entry.split()[2])
                    delivered_by_from[(by, from_)] += [content]
                    delivered_by[by] += [content]
                    events[process] += [('d', from_, content)]

        # Sets for performance
        broadcast_by_set = {i: set(arr) for (i, arr) in broadcast_by.items()}
        delivered_by_set = {i: set(arr) for (i, arr) in delivered_by.items()}
        delivered_by_from_set = {(i, j): set(arr) for ((i, j), arr) in delivered_by_from.items()}

        start_time = time.time()
        # BEB1 Validity: If a correct process broadcasts a message m, then every correct process eventually delivers m.
        for p in correct:
            for msg in broadcast_by_set[p]:
                for p1 in correct:
                    soft_assert(msg in delivered_by_set[p1], "BEB1 Violated. Correct %d broadcasted %s and correct %d did not receive it" % (p, msg, p1))
        print("BEB1 : {}".format(time.time() - start_time))
        start_time = time.time()

        # BEB2: No duplication
        for p in processes:
            for s in processes:
                delivered_by_p_f = delivered_by_from[(p, s)]
                soft_assert(len(delivered_by_p_f) == len(set(delivered_by_p_f)), "BEB2 Violated. Process %d delivered some messages from %d twice" % (p, s))
        print("BEB2 : {}".format(time.time() - start_time))
        start_time = time.time()
        
        # BEB3: No creation
        for p in processes:
            for p1 in processes:
                sent = broadcast_by_set[p]
                delivered = delivered_by_from_set[(p1, p)]
                for msg in delivered:
                    soft_assert(msg in sent, "BEB3 violated. Message %d was NOT send from %d and WAS delivered by %d" % (msg, p, p1))
        print("BEB3 : {}".format(time.time() - start_time))
        start_time = time.time()

        # URB4: Agreement. If a message m is delivered by some (correct/faulty) process, then m is eventually delivered by every correct process.
        all_delivered = [x for p in correct for x in delivered_by_set[p]]
        for msg in all_delivered:
            delivered_all = [p for p in processes if msg in delivered_by_set[p]]
            notdelivered_correct = [p for p in correct if msg not in delivered_by_set[p]]
            soft_assert(len(delivered_all) == 0 or len(notdelivered_correct) == 0, "URB4 Violated. Process %s delivered %d and correct %s did not deliver it" % (delivered_all, msg, notdelivered_correct))
        print("URB4 : {}".format(time.time() - start_time))
        start_time = time.time()

        # RB4 secondary check
        for p in correct:
            delivered_by_p = delivered_by_set[p]
            for p1 in correct:
                delivered_by_p1 = delivered_by_set[p1]
                for msg in delivered_by_p:
                    soft_assert(msg in delivered_by_p1)#, "RB4 Violated. Correct %d delivered %d and correct %d did not deliver it" % (p, msg, p1))
        print("RB4 : {}".format(time.time() - start_time))
        start_time = time.time()
        
        # CRB5: Causal delivery: For any message m1 that potentially caused a message m2, i.e., m1 -> m2 , no process delivers m2 unless it has already delivered m1.
        # (a) some process p broadcasts m1 before it broadcasts m2 ;
        # (b) some process p delivers m1 from a process (LOCALIZED) IT DEPENDS ON and subsequently broadcasts m2; or
        # (c) there exists some message m such that m1 -> m and m -> m2.
        # Process has dependencies dependencies[p] and itself

        # message dependencies: (sender, seq) -> [seq1, ..., seqN]
        msg_vc = {}

        # filling in vector clocks
        for p in processes:
            # current vector clock for a message (dependencies of a newly sent message)
            v_send = [0 for _ in range(n)]
            seqnum = 0

            # going over events
            for event in events[p]:
                # current message and type of event
                type_, msg = event[0], event[1:]

                # broadcast case: incrementing v_send
                if type_ == 'b':
                    # copying v_send
                    W = [x for x in v_send]

                    # filling in seqnum
                    W[p - 1] = seqnum

                    # incrementing seqnum
                    seqnum += 1

                    # copying W to msg_vc
                    msg_vc[msg] = [x for x in W]

                # delivery case: incrementing v_send if depend on the sender
                if type_ == 'd' and msg[0] in dependencies[p]:
                    v_send[msg[0] - 1] += 1

        # PROPERTY TEST: for each process, for each delivery, must have W <= V_recv
        for p in processes:
            # currently delivered messages by process
            v_recv = [0 for _ in range(n)]

            # loop over events
            for event in events[p]:
                # only care about deliveries here
                if event[0] != 'd': continue

                # parsing message = (sender, seq)
                msg = event[1:]

                # sanity check
                assert msg in msg_vc, "Must have a vector clock for %s" % str(msg)

                # property test
                soft_assert(vec_leq(msg_vc[msg], v_recv), "CRB5 violated: Process %d have delivered %s with vector clock W = %s having V_recv = %s" % (p, str(msg), str(msg_vc[msg]), str(v_recv)))

                # incrementing v_recv
                v_recv[msg[0] - 1] += 1

        print("CRB5 : {}".format(time.time() - start_time))
        
        # printing the last line with status
        print("INCORRECT" if were_errors else "CORRECT")

        return not were_errors

class StressTest:
    def __init__(self, procs, concurrency, attempts, attemptsRatio):
        self.processes = len(procs)
        self.processesInfo = dict()
        for (logicalPID, handle) in procs:
            self.processesInfo[logicalPID] = ProcessInfo(handle)
        self.concurrency = concurrency
        self.attempts = attempts
        self.attemptsRatio = attemptsRatio

        maxTerminatedProcesses = self.processes // 2 if self.processes % 2 == 1 else (self.processes - 1) // 2
        self.terminatedProcs = AtomicSaturatedCounter(maxTerminatedProcesses)

    def stress(self):
        selectProc = list(range(1, self.processes+1))
        random.shuffle(selectProc)

        selectOp = [ProcessState.STOPPED] * int(1000 * self.attemptsRatio['STOP']) + \
                    [ProcessState.RUNNING] * int(1000 * self.attemptsRatio['CONT']) + \
                    [ProcessState.TERMINATED] * int(1000 * self.attemptsRatio['TERM'])
        random.shuffle(selectOp)

        successfulAttempts = 0
        while successfulAttempts < self.attempts:
            proc = random.choice(selectProc)
            op = random.choice(selectOp)
            info = self.processesInfo[proc]

            with info.lock:
                if ProcessInfo.validStateTransition(info.state, op):

                    if op == ProcessState.TERMINATED:
                        reserved = self.terminatedProcs.reserve()
                        if reserved:
                            selectProc.remove(proc)
                        else:
                            continue

                    time.sleep(float(random.randint(50, 500)) / 1000.0)
                    info.handle.send_signal(ProcessInfo.stateToSignal(op))
                    info.state = op
                    successfulAttempts += 1
                    print("Sending {} to process {}".format(ProcessInfo.stateToSignalStr(op), proc))

                    # if op == ProcessState.TERMINATED and proc not in terminatedProcs:
                    #     if len(terminatedProcs) < maxTerminatedProcesses:

                    #         terminatedProcs.add(proc)

                    # if len(terminatedProcs) == maxTerminatedProcesses:
                    #     break

    def remainingUnterminatedProcesses(self):
        remaining = []
        for pid, info in self.processesInfo.items():
            with info.lock:
                if info.state != ProcessState.TERMINATED:
                    remaining.append(pid)

        return None if len(remaining) == 0 else remaining

    def terminatedProcesses(self):
        remaining = []
        for pid, info in self.processesInfo.items():
            with info.lock:
                if info.state == ProcessState.TERMINATED:
                    remaining.append(pid)

        return None if len(remaining) == 0 else remaining

    def terminateAllProcesses(self):
        for _, info in self.processesInfo.items():
            with info.lock:
                if info.state != ProcessState.TERMINATED:
                    if info.state == ProcessState.STOPPED:
                        info.handle.send_signal(ProcessInfo.stateToSignal(ProcessState.RUNNING))

                    info.handle.send_signal(ProcessInfo.stateToSignal(ProcessState.TERMINATED))

        return False

    def continueStoppedProcesses(self):
        for _, info in self.processesInfo.items():
            with info.lock:
                if info.state != ProcessState.TERMINATED:
                    if info.state == ProcessState.STOPPED:
                        info.handle.send_signal(ProcessInfo.stateToSignal(ProcessState.RUNNING))

    def run(self):
        if self.concurrency > 1:
            threads = [threading.Thread(target=self.stress) for _ in range(self.concurrency)]
            [p.start() for p in threads]
            [p.join() for p in threads]
        else:
            self.stress()

def startProcesses(processes, runscript, hostsFilePath, configFilePath, outputDir):
    runscriptPath = os.path.abspath(runscript)
    if not os.path.isfile(runscriptPath):
        raise Exception("`{}` is not a file".format(runscriptPath))

    if os.path.basename(runscriptPath) != 'run.sh':
        raise Exception("`{}` is not a runscript".format(runscriptPath))

    outputDirPath = os.path.abspath(outputDir)
    if not os.path.isdir(outputDirPath):
        raise Exception("`{}` is not a directory".format(outputDirPath))

    baseDir, _ = os.path.split(runscriptPath)
    bin_cpp = os.path.join(baseDir, "bin", "da_proc")
    bin_java = os.path.join(baseDir, "bin", "da_proc.jar")

    if os.path.exists(bin_cpp):
        cmd = [bin_cpp]
    elif os.path.exists(bin_java):
        cmd = ['java', '-jar', bin_java]
    else:
        raise Exception("`{}` could not find a binary to execute. Make sure you build before validating".format(runscriptPath))

    procs = []
    for pid in range(1, processes+1):
        cmd_ext = ['--id', str(pid),
                   '--hosts', hostsFilePath,
                   '--barrier', '{}:{}'.format(BARRIER_IP, BARRIER_PORT),
                   '--signal', '{}:{}'.format(SIGNAL_IP, SIGNAL_PORT),
                   '--output', os.path.join(outputDirPath, 'proc{:02d}.output'.format(pid)),
                   configFilePath]

        stdoutFd = open(os.path.join(outputDirPath, 'proc{:02d}.stdout'.format(pid)), "w")
        stderrFd = open(os.path.join(outputDirPath, 'proc{:02d}.stderr'.format(pid)), "w")


        procs.append((pid, subprocess.Popen(cmd + cmd_ext, stdout=stdoutFd, stderr=stderrFd)))

    return procs

def main(processes, messages, runscript, broadcastType, logsDir, testConfig):
    # Set tc for loopback
    tc = TC(testConfig['TC'])
    print(tc)

    # Start the barrier
    initBarrier = barrier.Barrier(BARRIER_IP, BARRIER_PORT, processes)
    initBarrier.listen()
    startTimesFuture = initBarrier.startTimesFuture()

    initBarrierThread = threading.Thread(target=initBarrier.wait)
    initBarrierThread.start()

    # Start the finish signal
    finishSignal = finishedSignal.FinishedSignal(SIGNAL_IP, SIGNAL_PORT, processes)
    finishSignal.listen()
    finishSignalThread = threading.Thread(target=finishSignal.wait)
    finishSignalThread.start()

    if broadcastType == "fifo":
        validation = FifoBroadcastValidation(processes, messages, logsDir)
    else:
        # Use the last argument (now it's `None` since it's not being use) to
        # pass any information that you think is relevant
        validation = LCausalBroadcastValidation(processes, messages, logsDir, None)

    hostsFile, configFile = validation.generateConfig()

    try:
        # Start the processes and get their PIDs
        procs = startProcesses(processes, runscript, hostsFile.name, configFile.name, logsDir)

        # Create the stress test
        st = StressTest(procs,
                        testConfig['ST']['concurrency'],
                        testConfig['ST']['attempts'],
                        testConfig['ST']['attemptsDistribution'])

        for (logicalPID, procHandle) in procs:
            print("Process with logicalPID {} has PID {}".format(logicalPID, procHandle.pid))


        initBarrierThread.join()
        print("All processes have been initialized.")

        st.run()
        print("StressTest is complete.")


        print("Resuming stopped processes.")
        st.continueStoppedProcesses()

        print("Waiting until all running processes have finished broadcasting.")
        finishSignalThread.join()

        for pid, startTs in OrderedDict(sorted(startTimesFuture.items())).items():
            print("Process {} finished broadcasting {} messages in {} ms".format(pid, messages, finishSignal.endTimestamps()[pid] - startTs))

        terminated = st.terminatedProcesses()
        with open(logsDir+"/crashed.log", "w") as f:
            if not terminated == None:
                f.write(" ".join([str(x) for x in terminated]))
        
        unterminated = st.remainingUnterminatedProcesses()
        if unterminated is not None:
            input('Hit `Enter` to terminate the remaining processes with logicalPIDs {}.'.format(unterminated))
            st.terminateAllProcesses()

        mutex = threading.Lock()

        def waitForProcess(logicalPID, procHandle, mutex):
            procHandle.wait()

            with mutex:
                print("Process {} exited with {}".format(logicalPID, procHandle.returncode))

        # Monitor which processes have exited
        monitors = [threading.Thread(target=waitForProcess, args=(logicalPID, procHandle, mutex)) for (logicalPID, procHandle) in procs]
        [p.start() for p in monitors]
        [p.join() for p in monitors]

        input('Hit `Enter` to validate the output')
        print("Result of validation: {}".format(validation.checkAll()))

    finally:
        if procs is not None:
            for _, p in procs:
                p.kill()

if __name__ == "__main__":
    were_errors = False
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-r",
        "--runscript",
        required=True,
        dest="runscript",
        help="Path to run.sh",
    )

    parser.add_argument(
        "-b",
        "--broadcast",
        choices=["fifo", "lcausal"],
        required=True,
        dest="broadcastType",
        help="Which broadcast implementation to test",
    )

    parser.add_argument(
        "-l",
        "--logs",
        required=True,
        dest="logsDir",
        help="Directory to store stdout, stderr and outputs generated by the processes",
    )

    parser.add_argument(
        "-p",
        "--processes",
        required=True,
        type=int,
        dest="processes",
        help="Number of processes that broadcast",
    )

    parser.add_argument(
        "-m",
        "--messages",
        required=True,
        type=int,
        dest="messages",
        help="Maximum number (because it can crash) of messages that each process can broadcast",
    )

    results = parser.parse_args()

    testConfig = {
        # Network configuration using the tc command
        'TC': {
            'delay': ('200ms', '50ms'),
            'loss': ('10%', '25%'),
            'reordering': ('25%', '50%')
        },

        # StressTest configuration
        'ST': {
            'concurrency' : 8, # How many threads are interferring with the running processes
            'attempts' : 8, # How many interferring attempts each threads does
            'attemptsDistribution' : { # Probability with which an interferring thread will
                'STOP': 0.48,          # select an interferring action (make sure they add up to 1)
                'CONT': 0.48,
                'TERM':0.04
            }
        }
    }
    # testConfig = {
    #     # Network configuration using the tc command
    #     'TC': {
    #         'delay': ('0ms', '0ms'),
    #         'loss': ('0%', '0%'),
    #         'reordering': ('0%', '0%')
    #     },

    #     # StressTest configuration
    #     'ST': {
    #         'concurrency' : 8, # How many threads are interferring with the running processes
    #         'attempts' : 0, # How many interferring attempts each threads does
    #         'attemptsDistribution' : { # Probability with which an interferring thread will
    #             'STOP': 0.48,          # select an interferring action (make sure they add up to 1)
    #             'CONT': 0.48,
    #             'TERM':0.04
    #         }
    #     }
    # }

    main(results.processes, results.messages, results.runscript, results.broadcastType, results.logsDir, testConfig)
