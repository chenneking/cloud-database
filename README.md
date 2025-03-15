# Cloud Database
This project implements a distributed key-value store written entirely using Java's Core Libraries. It consists of three components, which are described in further detail below: a kv-client, a kv-server (node), and an ecs-server orchestrating the multi-node system. Please refer to report/Advanced Distributed Load Balancing.pdf for a detailed description.

## KV-Client
Connects to one of the KV-Servers (or is redirected to another KV-Server) and interacts with the key-value store using a custom set of commands.

## KV-Server
Holds a specific subset of key-value pairs, as specified via the ECS-Server. Also serves client requests to access said data.

## ECS-Server
Orchestrates the spawning and de-spawning/failure of KV-Servers, splitting key-ranges among the set of available nodes using consistent hashing.
