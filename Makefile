# install the C++ garbarge collector from  https://github.com/ivmai/bdwgc/
GC_HOME = ${HOME}/system/gc-8.2.2

all:
	g++ -O3 -fopenmp -fpermissive -w -DNDEBUG -Iinclude -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

clean: 
	/bin/rm -f src/main/cpp/*~ include/*~ lib/libdiablo.a
