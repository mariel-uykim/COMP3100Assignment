## **DS-Sim Job Scheduler (Client-side)**
#### COMP3100: Distributed Systems - Assignment 
Submitted by: Mariel Anne Uykim (46129448)

---
**Overview**
DS Sim is a discrete event simulator that schedules a list of given jobs to a cluster of servers with different specifications and capabilities. This project is based off of [this](https://github.com/distsys-MQ/ds-sim) DS Sim project. This project implements the client-side simulator of DS Sim. It schedules different jobs handed to it in a round robin fashion among the servers with the largest server type (based off on the number of cores). 

**Instructions**
1. Run server `$ ./ds-server -c ../pathway-to-config/xm-file -n -v brief` 
2. Run client `$ java DSClient`




