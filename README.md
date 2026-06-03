# Intra-LAN Implementation for Agent-Based Task Allocation System

Run the NATS-server with:
```bash
$ sudo nats-server -c /path/to/your/nats/conf/file.conf
```

In another terminal window, run the task allocator:
```bash
$ python3 node.py
```