cpu:
	mpic++ -O3 -fopenmp -DNDEBUG -Iinclude -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

debug-cpu:
	mpic++ -g3 -fopenmp -Iinclude -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

ptx:
	g++ -O3 -c src/main/cpp/*.cpp -fopenacc -foffload=nvptx-none -fcf-protection=none -fno-stack-protector -no-pie -I${MPI_HOME}/include -Iinclude -L${CUDA_HOME}/lib64 -lcudart -I${CUDA_HOME}/include -L${MPI_HOME}/lib -lmpi -lcuda
	ar rcs lib/libdiablo.a *.o
	rm *.o

ptx-hpc:
	mpicxx -O3 -c src/main/cpp/*.cpp -acc -gpu=cc80 -Iinclude -lcuda -lcudart
	ar rcs lib/libdiablo.a *.o
	rm *.o

gpu:
	g++ -O3 -fopenacc -foffload=nvptx-none -fcf-protection=none -fno-stack-protector -no-pie -I${MPI_HOME}/include -Iinclude -L${MPI_HOME}/lib -lmpi -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

debug-gpu:
	g++ -g3 -fopenacc -foffload=nvptx-none -fcf-protection=none -fno-stack-protector -no-pie -I${MPI_HOME}/include -Iinclude -L${MPI_HOME}/lib -lmpi -c src/main/cpp/*.cpp
	ar rcs lib/libdiablo.a *.o
	rm *.o

clean: 
	/bin/rm -f src/main/cpp/*~ include/*~ lib/libdiablo.a
