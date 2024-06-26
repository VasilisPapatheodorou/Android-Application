﻿# Distributed Accommodation Management System

## AUEB | Web Development | Semester 8 | 2023 - 2024

## Overview
This project implements a Distributed Accommodation Management System, consisting of multiple components: a master node, worker nodes, and a reducer node. The system allows users to perform various operations related to managing accommodations, such as adding accommodations, searching for accommodations, renting accommodations, and more. 

## Components

### 1. Master Node
- The master node is responsible for coordinating client requests and distributing tasks to worker nodes.
- It communicates with client applications using TCP connections, listening for incoming requests on a specific port.
- Upon receiving a request, it spawns a new thread to handle the client connection asynchronously, allowing multiple clients to interact with the system simultaneously.
- Based on the user's choice, it communicates with worker nodes to perform operations such as adding accommodation, searching accommodation, etc.
- It aggregates results received from worker nodes and sends them to the reducer node for final processing.

### 2. Worker Nodes
- Worker nodes execute tasks assigned by the master node.
- They maintain shared memory to store accommodation data and process requests received from the master node.
- Worker nodes establish TCP connections with the master node to receive tasks and send back processed results.
- Depending on the operation requested by the master node, worker nodes perform tasks such as adding accommodation, filtering accommodations, adding booking details, etc.
- After processing the data, worker nodes send the results back to the master node for aggregation.

### 3. Reducer Node
- The reducer node aggregates results received from multiple worker nodes.
- It listens for TCP connections from worker nodes and receives processed data.
- After receiving data from worker nodes, it aggregates the results and performs any final processing if necessary.
- The aggregated results are then sent back to the master node for presenting to the user.


## Instructions for Running the Project
1. Compile all Java files in the project

2. Run the components in the following order:
- Start the reducer node:
  ```
  java reducer
  ```
- Start the worker nodes:
  ```
  java workerNode
  ```
- Start the master node:
  ```


## Additional Notes
- Ensure that all components are running and properly connected to each other to ensure the smooth functioning of the system.
- The project uses TCP connections for communication between nodes, ensuring reliable and ordered delivery of data.
- Asynchronous logic using threads is employed in the master node to handle multiple client connections concurrently, enhancing system responsiveness and scalability.
- Make sure to handle any exceptions or errors that may occur during the execution of the system.
