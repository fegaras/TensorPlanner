all:
	g++ -g -Iinclude -c src/main/cpp/*.cpp
	ar rcs lib/TensorPlanner.a *.o
	rm *.o

clean: 
	/bin/rm -f src/main/cpp/*~ include/*~ lib/TensorPlanner.a
