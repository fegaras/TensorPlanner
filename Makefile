all:
	#don't use -O
	mpic++ -fopenmp -fpermissive -w -DNDEBUG -Iinclude -I${GC_HOME}/include -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

debug:
	mpic++ -g -fopenmp -fpermissive -w -Iinclude -I${GC_HOME}/include -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

clean: 
	/bin/rm -f src/main/cpp/*~ include/*~ lib/libdiablo.a
