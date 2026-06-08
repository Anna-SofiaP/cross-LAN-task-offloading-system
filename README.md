# Intra-LAN Implementation for Agent-Based Task Allocation System

Run the NATS-server with:
```bash
$ sudo nats-server -c /path/to/your/nats/conf/file.conf
```

In another terminal window, run the task allocator:
```bash
$ python3 node.py
```

In this branch the idea is to test how the following workflow and application logic would work:
- Task originator sends task_request. If task request type is PRIVATE, it is sent via ZMQ, otherwise via NATS.
    - The task request is attempted 3 times.
- As a response, agents send their bids. No ack messages anymore!