tasker:
- Control protocol - connect to each worker, gather its logs, pause, resume, change concurrency....
- Dashboard improvments -> drill down each worker, get all its configuration.......
- Add worker statistics, and collect them in the controller
- add retry in X time, or at date. https://redislabs.com/ebook/redis-in-action/part-2-core-concepts-2/chapter-6-application-components-in-redis/6-4-task-queues/6-4-2-delayed-tasks
- add multi tasks storage engine via the redis self.storage.set_value() self.storage.get_value()
- apply_async task with until_success flag.
- add faulthandler
- add task a prefix/label
monitor:
- update the queues with multiple queues
- fix the workers table -> sort, and improve the ui/ux
- add pyte to remote control a worker machine
- improve angular performance with large lists
