from bbq.core.task import EchoTask

tasks = list()
for i in range(5):
    new_task = EchoTask(str(i + 1))
    if len(tasks) > 0:
        tasks[-1].set_downstream(new_task)
    tasks.append(new_task)
