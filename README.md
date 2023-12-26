# YAKt (Yet Another KRaft) ⚙️
- YAKt is our semester project for the Big Data (UE21CS343AB2) course. 
- It involves developing the KRaft protocol to metadata for the Kafka system.


## What is KRaft ? 

- KRaft is a event based, distributed metadata management system that was written to replace Zookeeper in the ecosystem of Kafka.
- It uses Raft as an underlying consensus algorithm to do log replication and manage consistency of state.

## Project Objectives and Outcomes

- The Raft Consensus algorithm and concepts of Leader Election, Log Replication, Fault Tolerance
- ZAB Consensus algorithm
- Kraft and Zookeeper Architecture and its nuances
- Event driven Architecture
- Kafka Architecture
- Outcomes

- A robust system that mimics the working of KRaft
- A project with a real world use-case


## Steps to Run the Project

```
cd kraft
```
```
pip3 install -r requirements.txt
```
```
python3 raft.py 1000 1001 1002
```

```
python3 raft.py 1001 1001 1002
```

```
python3 raft.py 1002 1000 1001
```

- You can Now Make your API endpoint via the GET or POST Messages!

# Contributors:

1. Jugal Kothari PES1UG21CS251
2. Karthik Namboori PES1UG21CS269
3. Prerana Kulkarni PES1UG21CS451
4. Shreya N Palavalli PES1UG21CS575
