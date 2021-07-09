# aerospike insert with threads
 - threaded example of sending n records to a local / remote instance
 - to send 100,000 records to a local instance and time it.
   -  ```./insert.sh 10 1000 "127.0.0.1" insurance rep1```
 - to ssend 1m records using 150 threads to a 3 node cluster using private IP addresses 
   - ```./insert.sh 150 1000000 "10.0.0.61,10.0.1.33,10.0.2.147" test set1```
