/*
 * Copyright © 2023-2024 University of Texas at Arlington
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

int getCoordinator();

bool isCoordinator();

int check_communication ();

void send_data ( int rank, void* data, int opr_id, int tag );

void send_long ( int rank, long data, int tag );

void send_data ( int rank, void* data, int size );

void receive_data ( void* buffer );

void mpi_startup ( int argc, char* argv[], int block_dim_size );

void mpi_finalize ();

void mpi_barrier_no_recovery ();

void mpi_barrier ();

bool wait_all ( bool b );

void kill_receiver ();

void run_receiver ();

int mpi_abort ();

void reset_accumulator ();

bool accumulator_exit ();

