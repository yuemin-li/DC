
1. modify the path in the Makefile to locate your own zookeeper.jar

2. make the file

3. run the java program with classpathes and proper args.
java -cp ./zookeeper-3.4.6/lib/*:./zookeeper-3.4.6/zookeeper-3.4.6.jar:. Client Layer1.txt Layer2.txt 127.0.0.1 2181 Test_Input.txt

4. the DNS_log.txt is the outputfile with resolution results.
