# Intra-LAN Implementation for Agent-Based Task Allocation System

This Cross-LAN privacy-, reliability-, and priority-aware task offloading system is based on a task allocation system originally designed by Karthikeyan, Mikkonen and Mäkitalo[^1].

## How to run the system (more information coming soon...)

Run the NATS-server with:
```bash
$ sudo nats-server -c /path/to/your/nats/conf/file.conf
```

In another terminal window, run the task allocator:
```bash
$ python3 node.py
```

## References:
[^1]: Karthikeyan, Dinesh Kumar and Mikkonen, Tommi and Mäkitalo, Niko, From Reactive Scheduling to Adaptive Coordination: A Predictive - Cognitive System for Resource‑Aware Task Placement in Edge Environments. Available at SSRN: https://ssrn.com/abstract=6902610 or http://dx.doi.org/10.2139/ssrn.6902610 