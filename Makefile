all:
	mpic++ -O3 -fopenmp -DNDEBUG -Iinclude -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

debug:
	mpic++ -g -fopenmp -Iinclude -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

clean: 
	/bin/rm -f src/main/cpp/*~ include/*~ lib/libdiablo.a
