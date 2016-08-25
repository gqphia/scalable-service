all: Cache.class Server.class

%.class: %.java
	javac $<

clean:
	rm -f *.class
