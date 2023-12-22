/*
 * Copyright © 2023 University of Texas at Arlington
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "tensor.h"

extern vector<void*(*)(void*)> functions;

vector<int>* range ( long n1, long n2, long n3 );

bool inRange ( long i, long n1, long n2, long n3 );

int loadOpr ( void* block, void* coord );
int loadOpr ( void* block, int coord );

int pairOpr ( int x, int y, void* coord, int destr );
int pairOpr ( int x, int y, int coord, int destr );

int applyOpr ( int x, int fnc, void* args, void* coord, int cost, int destr );
int applyOpr ( int x, int fnc, void* args, int coord, int cost, int destr );

int reduceOpr ( const vector<int>* s, bool valuep, int op, void* coord, int cost, int destr );
int reduceOpr ( const vector<int>* s, bool valuep, int op, int coord, int cost, int destr );

void* evalTopDown ( void* plan );

void* eval ( void* plan );
