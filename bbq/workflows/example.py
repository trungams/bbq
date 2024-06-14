from bbq.core.task import BuildCppTask, RunBashTask, RunPythonTask

task_A = BuildCppTask("A", ["A/a.cpp"], ["out"])
task_B = RunBashTask("B", ["B/b.sh"], ["b.txt"])
task_C = RunPythonTask("C", ["C/c.py"])
task_D = RunBashTask("D", ["D/d.sh"], ["d.txt"])

task_A >> task_C
task_B >> task_C
task_C >> task_D
tasks = [task_A, task_B, task_C, task_D]
