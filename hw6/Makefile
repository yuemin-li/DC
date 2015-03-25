CLASSPATH=./zookeeper-3.4.6/zookeeper-3.4.6.jar:.

target:
	javac Clock.java -d ./ -classpath ./
	javac Currency.java -d ./ -classpath ./
	javac LocalClock.java -d ./ -classpath ./
	javac TotalOrderZK.java -d ./ -classpath $(CLASSPATH)
	javac Dispatcher.java -d ./ -classpath $(CLASSPATH)
	javac Worker.java -d ./ -classpath $(CLASSPATH)
