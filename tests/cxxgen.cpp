#include "runtime.h"

#include "cuda_util.h"
#include <cuda.h>

int  N;

int  M;

int  max_rand;

float  upper_bound;

vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>* _v_1 = new vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>();

void _v_33 () {
   for ( int _v_0 = 0; _v_0 <= ((N-1)/4096); _v_0 += 1 )
   { vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>* _v_3 = new vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>();
      for ( int _v_2 = 0; _v_2 <= ((M-1)/4096); _v_2 += 1 )
         { int _v_4 = (((((_v_2+1)*4096)>M)) ? (M%4096) : 4096);
            int _v_5 = (((((_v_0+1)*4096)>N)) ? (N%4096) : 4096);
            float zero = 0.0f;
            Vec<float>* buffer = array_buffer_dense((_v_5*_v_4),zero);
            { auto _v_34 = buffer->buffer(); 
               int device_id = get_gpu_id();
               setDevice(device_id);
#pragma acc parallel deviceptr(_v_34)
#pragma acc loop tile(32,32)
            for ( int _v_6 = 0; _v_6 <= (_v_5-1); _v_6 += 1 )
               for ( int _v_7 = 0; _v_7 <= (_v_4-1); _v_7 += 1 )
                  _v_34[(((((_v_0*4096)+_v_6)%4096)*_v_4)+(((_v_2*4096)+_v_7)%4096))] = 1.0f;
            }
;
            if (((_v_2*4096)<=M))
               append1(_v_3,new tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>(new tuple<uintptr_t,uintptr_t>(_v_0,_v_2),loadOpr(new tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>(new tuple<uintptr_t,uintptr_t>(_v_0,_v_2),new tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>(new tuple<uintptr_t,uintptr_t>(_v_5,_v_4),nullptr,buffer)),new tuple<uintptr_t,uintptr_t>(_v_0,_v_2),new vector<int>({ 10, 2, 10, 2, 0, 0, 10, 3, 10, 2, 0, 0, 10, 0, 11, 3 })))); };
      if (((_v_0*4096)<=N))
         append(_v_1,_v_3); };
}


tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>*  Az;

vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>* _v_9 = new vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>();

void _v_35 () {
   for ( int _v_8 = 0; _v_8 <= ((N-1)/4096); _v_8 += 1 )
   { vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>* _v_11 = new vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>();
      for ( int _v_10 = 0; _v_10 <= ((M-1)/4096); _v_10 += 1 )
         { int _v_12 = (((((_v_10+1)*4096)>M)) ? (M%4096) : 4096);
            int _v_13 = (((((_v_8+1)*4096)>N)) ? (N%4096) : 4096);
            float zero = 0.0f;
            Vec<float>* buffer = array_buffer_dense((_v_13*_v_12),zero);
            { auto _v_36 = buffer->buffer(); 
               int device_id = get_gpu_id();
               setDevice(device_id);
#pragma acc parallel deviceptr(_v_36)
#pragma acc loop tile(32,32)
            for ( int _v_14 = 0; _v_14 <= (_v_13-1); _v_14 += 1 )
               for ( int _v_15 = 0; _v_15 <= (_v_12-1); _v_15 += 1 )
                  _v_36[(((((_v_8*4096)+_v_14)%4096)*_v_12)+(((_v_10*4096)+_v_15)%4096))] = 2.0f;
            }
;
            if (((_v_10*4096)<=M))
               append1(_v_11,new tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>(new tuple<uintptr_t,uintptr_t>(_v_8,_v_10),loadOpr(new tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>(new tuple<uintptr_t,uintptr_t>(_v_8,_v_10),new tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>(new tuple<uintptr_t,uintptr_t>(_v_13,_v_12),nullptr,buffer)),new tuple<uintptr_t,uintptr_t>(_v_8,_v_10),new vector<int>({ 10, 2, 10, 2, 0, 0, 10, 3, 10, 2, 0, 0, 10, 0, 11, 3 })))); };
      if (((_v_8*4096)<=N))
         append(_v_9,_v_11); };
}


tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>*  Bz;

int  reps;

int  iter;

void _v_37 () {
   while ((iter<reps))
{ vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>* _v_24 = new vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>();
   for ( tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>* _v_23: *get<2>(*Az) )
      { tuple<uintptr_t,uintptr_t>* _v_25 = get<0>(*_v_23);
         append1(_v_24,new tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>(get<1>(*_v_25),new tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>(new tuple<uintptr_t,uintptr_t>(get<0>(*_v_25),get<1>(*_v_25)),applyOpr(get<1>(*_v_23),1,nullptr,get<1>(*_v_25),0,new vector<int>({ 10, 2, 0, 10, 2, 10, 2, 0, 0, 10, 3, 10, 2, 0, 0, 10, 0, 11, 3 }))))); };
   vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>* _v_27 = new vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>();
   for ( tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>* _v_26: *get<2>(*Bz) )
      { tuple<uintptr_t,uintptr_t>* _v_28 = get<0>(*_v_26);
         append1(_v_27,new tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>(get<0>(*_v_28),new tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>(new tuple<uintptr_t,uintptr_t>(get<0>(*_v_28),get<1>(*_v_28)),applyOpr(get<1>(*_v_26),2,nullptr,get<0>(*_v_28),0,new vector<int>({ 10, 2, 0, 10, 2, 10, 2, 0, 0, 10, 3, 10, 2, 0, 0, 10, 0, 11, 3 }))))); };
   vector<tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<tuple<uintptr_t,uintptr_t>*,tuple<uintptr_t,uintptr_t>*>*,uintptr_t>*>*>* _v_19 = new vector<tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<tuple<uintptr_t,uintptr_t>*,tuple<uintptr_t,uintptr_t>*>*,uintptr_t>*>*>();
   for ( tuple<uintptr_t,tuple<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>* _v_18: *join(_v_24,_v_27) )
      { tuple<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>* _v_20 = get<1>(*_v_18);
         tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>* _v_22 = get<1>(*_v_20);
         tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>* _v_21 = get<0>(*_v_20);
         tuple<uintptr_t,uintptr_t>* _v_29 = get<0>(*_v_22);
         tuple<uintptr_t,uintptr_t>* _v_30 = get<0>(*_v_21);
         if ((get<1>(*_v_30)==get<0>(*_v_29)))
            append1(_v_19,new tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<tuple<uintptr_t,uintptr_t>*,tuple<uintptr_t,uintptr_t>*>*,uintptr_t>*>(new tuple<uintptr_t,uintptr_t>(get<0>(*_v_30),get<1>(*_v_29)),new tuple<tuple<tuple<uintptr_t,uintptr_t>*,tuple<uintptr_t,uintptr_t>*>*,uintptr_t>(new tuple<tuple<uintptr_t,uintptr_t>*,tuple<uintptr_t,uintptr_t>*>(new tuple<uintptr_t,uintptr_t>(get<0>(*_v_30),get<1>(*_v_30)),new tuple<uintptr_t,uintptr_t>(get<0>(*_v_29),get<1>(*_v_29))),applyOpr(pairOpr(get<1>(*_v_21),get<1>(*_v_22),get<0>(*_v_18),new vector<int>({ 10, 2, 0, 10, 2, 10, 2, 10, 2, 0, 0, 10, 3, 10, 2, 0, 0, 10, 0, 11, 3, 10, 2, 10, 2, 0, 0, 10, 3, 10, 2, 0, 0, 10, 0, 11, 3 })),0,nullptr,new tuple<uintptr_t,uintptr_t>(get<0>(*_v_30),get<1>(*_v_29)),3,new vector<int>({ 10, 2, 10, 2, 0, 0, 10, 3, 10, 2, 0, 0, 10, 0, 11, 3 }))))); };
   vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>* _v_17 = new vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>();
   for ( tuple<tuple<uintptr_t,uintptr_t>*,vector<tuple<tuple<tuple<uintptr_t,uintptr_t>*,tuple<uintptr_t,uintptr_t>*>*,uintptr_t>*>*>* _v_16: *groupByKey(_v_19) )
      { vector<uintptr_t>* _v_32 = new vector<uintptr_t>();
         for ( tuple<tuple<tuple<uintptr_t,uintptr_t>*,tuple<uintptr_t,uintptr_t>*>*,uintptr_t>* _v_31: *get<1>(*_v_16) )
            append1(_v_32,get<1>(*_v_31));
         append1(_v_17,new tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>(get<0>(*_v_16),reduceOpr(_v_32,false,3,get<0>(*_v_16),1,new vector<int>({ 10, 2, 10, 2, 0, 0, 10, 3, 10, 2, 0, 0, 10, 0, 11, 3 })))); };
   Az = new tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>(new tuple<uintptr_t,uintptr_t>(N,M),nullptr,_v_17);
   iter += 1; }
;
}


auto _v_40 () {
   return elem(Az);
}


vector<tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>* _v_53 ( tuple<uintptr_t,tuple<tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>*>* _v_41 ) {
   vector<tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>* _v_42 = new vector<tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>();
   tuple<tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>* _v_43 = get<1>(*_v_41);
   tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>* _v_46 = get<1>(*_v_43);
   tuple<uintptr_t,uintptr_t>* _v_47 = get<0>(*_v_46);
   tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>* _v_44 = get<0>(*_v_43);
   tuple<uintptr_t,uintptr_t>* _v_45 = get<0>(*_v_44);
   Vec<float>* _v185 = array_buffer_dense(((((((get<0>(*_v_45)+1)*4096)>N)) ? (N%4096) : 4096)*(((((get<1>(*_v_47)+1)*4096)>M)) ? (M%4096) : 4096)),0.0f);
   tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>* _v_48 = get<1>(*_v_44);
   tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>* _v_49 = get<1>(*_v_46);
   auto _v_57 = _v185->buffer();
auto _v_61 = get<2>(*_v_49)->buffer();
auto _v_63 = get<2>(*_v_48)->buffer();

   int device_id = get_gpu_id();
   setDevice(device_id);
   
    CUmodule cuModule;
    CUfunction cuFunction;
    std::string ptx = loadPTX("mlir_output.ptx");
    cuModuleLoadDataEx(&cuModule, ptx.c_str(), 0, 0, 0);
    cuModuleGetFunction(&cuFunction, cuModule, "_v_67_kernel");
    launchCudaKernel(cuFunction, device_id, _v_57, _v_61, _v_63);
;
   if ((get<1>(*_v_45)==get<0>(*_v_47)))
      append1(_v_42,new tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>(new tuple<uintptr_t,uintptr_t>(get<0>(*_v_45),get<1>(*_v_47)),new tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>(new tuple<uintptr_t,uintptr_t>((((((get<0>(*_v_45)+1)*4096)>N)) ? (N%4096) : 4096),(((((get<1>(*_v_47)+1)*4096)>M)) ? (M%4096) : 4096)),nullptr,_v185)));
   return _v_42;
}

vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>*>* _v_71 ( tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>* _v_68 ) {
   vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>*>* _v_69 = new vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>*>();
   tuple<uintptr_t,uintptr_t>* _v_70 = get<0>(*_v_68);
   append1(_v_69,new tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>(get<1>(*_v_70),new tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>(new tuple<uintptr_t,uintptr_t>(get<0>(*_v_70),get<1>(*_v_70)),get<1>(*_v_68))));
   return _v_69;
}

vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>*>* _v_75 ( tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>* _v_72 ) {
   vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>*>* _v_73 = new vector<tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>*>();
   tuple<uintptr_t,uintptr_t>* _v_74 = get<0>(*_v_72);
   append1(_v_73,new tuple<uintptr_t,tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>*>(get<0>(*_v_74),new tuple<tuple<uintptr_t,uintptr_t>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>(new tuple<uintptr_t,uintptr_t>(get<0>(*_v_74),get<1>(*_v_74)),get<1>(*_v_72))));
   return _v_73;
}

float _v_81 ( tuple<float,float>* _v_79 ) {
   float  _v_80;
   _v_80 = (get<0>(*_v_79)+get<1>(*_v_79));
   return _v_80;
}

tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>* _v_78 ( tuple<tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*,tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*>* _v_76 ) {
   tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>*  _v_77;
   _v_77 = new tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,Vec<float>*>(get<0>(*get<0>(*_v_76)),get<1>(*get<0>(*_v_76)),merge_tensors(get<2>(*get<0>(*_v_76)),get<2>(*get<1>(*_v_76)),&_v_81,0.0f));
   return _v_77;
}

int main ( int argc, char* argv[] ) {
startup(argc,argv,4096);
functions.push_back((void*(*)(void*))&_v_53);
functions.push_back((void*(*)(void*))&_v_71);
functions.push_back((void*(*)(void*))&_v_75);
functions.push_back((void*(*)(void*))&_v_78);
N = atoi(argv[0+1]);
M = N;
max_rand = 1000000;
upper_bound = 1000000.0f;
_v_33();
Az = new tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>(new tuple<uintptr_t,uintptr_t>(N,M),nullptr,_v_1);
_v_35();
Bz = new tuple<tuple<uintptr_t,uintptr_t>*,nullptr_t,vector<tuple<tuple<uintptr_t,uintptr_t>*,uintptr_t>*>*>(new tuple<uintptr_t,uintptr_t>(N,M),nullptr,_v_9);
reps = atoi(argv[1+1]);
iter = 0;
_v_37();
for ( auto _v_38: *_v_40() ) { schedule(_v_38); eval(_v_38); collect(_v_38); }
mpi_finalize();
return 0;
}
