all:
	mpic++ -O3 -fopenmp -foffload=nvptx-none -fcf-protection=none -fno-stack-protector -no-pie -DNDEBUG -fpermissive -Iinclude -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

debug:
	mpic++ -g -fopenmp -foffload=nvptx-none -fcf-protection=none -fno-stack-protector -no-pie -fpermissive -Iinclude -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

clean: 
	/bin/rm -f src/main/cpp/*~ include/*~ lib/libdiablo.a
