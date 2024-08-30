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

#include "tensor.h"
#include <sstream>
#include <cstring>
#include <stdint.h>

void info(const char *fmt, ...);

extern int executor_rank;

void copy_data(char *data, const char *buffer, size_t len, int n)
{
  if (is_GPU())
  {
    int device_id = get_gpu_id();
#pragma omp target teams device(device_id) is_device_ptr(buffer,data)
#pragma omp parallel for
    for (int i = 0; i < n; i++)
      data[i] = buffer[i];
  }
  else
  {
    memcpy(data, buffer, len);
  }
}

void put_data(char *buffer, const char *data, size_t len, int n)
{
  if (is_GPU())
  {
    int device_id = get_gpu_id();
    int host_id = omp_get_initial_device();
    omp_target_memcpy(buffer, data, len, 0, 0, device_id, host_id);
  }
  else
  {
    memcpy(buffer, data, len);
  }
}

int serialize(const void *data, char *buffer, vector<int> *encoded_type, int loc, int *pos)
{
  uintptr_t i;
  switch ((*encoded_type)[loc])
  {
  case 0:
  case 1: // index
    i = (uintptr_t)data;
    put_data(buffer + (*pos), (const char*)&i, sizeof(uintptr_t), 1);
    *pos = *pos + sizeof(uintptr_t);
    return loc + 1;
  case 10:
  { // tuple
    switch ((*encoded_type)[loc + 1])
    {
    case 0:
      return loc + 2;
    case 2:
    {
      auto x = (tuple<void *, void *> *)data;
      int l2 = serialize(get<0>(*x), buffer, encoded_type, loc + 2, pos);
      return serialize(get<1>(*x), buffer, encoded_type, l2, pos);
    }
    case 3:
    {
      auto x = (tuple<void *, void *, void *> *)data;
      int l2 = serialize(get<0>(*x), buffer, encoded_type, loc + 2, pos);
      int l3 = serialize(get<1>(*x), buffer, encoded_type, l2, pos);
      return serialize(get<2>(*x), buffer, encoded_type, l3, pos);
    }
    default:
      info("Unknown tuple: %d", (*encoded_type)[loc + 1]);
    }
  }
  case 11: // Vec
    switch ((*encoded_type)[loc + 1])
    {
    case 0:
    {
      auto x = (Vec<int> *)data;
      int n = x->size();
      put_data(buffer + (*pos), (const char *)&n, sizeof(int), 1);
      *pos = *pos + sizeof(int);
      int *block_data = x->buffer();
      copy_data(buffer + (*pos), (const char *)block_data, sizeof(int) * n, n);
      *pos = *pos + (sizeof(int) * n);
      return loc + 2;
    }
    case 1:
    {
      auto x = (Vec<long> *)data;
      int n = x->size();
      put_data(buffer + (*pos), (const char *)&n, sizeof(int), 1);
      *pos = *pos + sizeof(int);
      long *block_data = x->buffer();
      copy_data(buffer + (*pos), (const char *)block_data, sizeof(long) * n, n);
      *pos = *pos + (sizeof(long) * n);
      return loc + 2;
    }
    case 3:
    {
      auto x = (Vec<double> *)data;
      int n = x->size();
      put_data(buffer + (*pos), (const char *)&n, sizeof(int), 1);
      *pos = *pos + sizeof(int);
      double *block_data = x->buffer();
      copy_data(buffer + (*pos), (const char *)block_data, sizeof(double) * n, n);
      *pos = *pos + (sizeof(double) * n);
      return loc + 2;
    }
    default:
      info("Unknown Vec: %d", (*encoded_type)[loc + 1]);
    }
  default:
    return loc + 1;
  }
}

int serialize(void *data, char *buffer, vector<int> *encoded_type)
{
  int pos = 0;
  serialize(data, buffer, encoded_type, 0, &pos);
  return pos;
}

void get_data(char *data, const char *buffer, size_t len, int n)
{
  if (is_GPU())
  {
    int device_id = get_gpu_id();
    int host_id = omp_get_initial_device();
    omp_target_memcpy(data, buffer, len, 0, 0, host_id, device_id);
  }
  else
  {
    memcpy(data, buffer, len);
  }
}

int deserialize(void *&data, const char *buffer, vector<int> *encoded_type, int loc, int *pos)
{
  uintptr_t i;
  switch ((*encoded_type)[loc])
  {
  case 0:
  case 1: // index
    get_data((char*)&i, buffer + (*pos), sizeof(uintptr_t), 1);
    data = (void*)i;
    *pos = *pos + sizeof(uintptr_t);
    return loc + 1;
  case 10:
  { // tuple
    switch ((*encoded_type)[loc + 1])
    {
    case 0:
      return loc + 2;
    case 2:
    {
      void *x1, *x2;
      int l2 = deserialize(x1, buffer, encoded_type, loc + 2, pos);
      int l3 = deserialize(x2, buffer, encoded_type, l2, pos);
      data = new tuple<void *, void *>(x1, x2);
      return l3;
    }
    case 3:
    {
      void *x1, *x2, *x3;
      int l2 = deserialize(x1, buffer, encoded_type, loc + 2, pos);
      int l3 = deserialize(x2, buffer, encoded_type, l2, pos);
      int l4 = deserialize(x3, buffer, encoded_type, l3, pos);
      data = new tuple<void *, void *, void *>(x1, x2, x3);
      return l4;
    }
    default:
      info("Unknown tuple: %d", (*encoded_type)[loc + 1]);
    }
  }
  case 11: // Vec
    switch ((*encoded_type)[loc + 1])
    {
    case 0:
    {
      int len;
      get_data((char*)&len, buffer + (*pos), sizeof(int), 1);
      *pos = *pos + sizeof(int);
      Vec<int> *x = new Vec<int>(len);
      int *block_data = x->buffer();
      copy_data((char*)block_data, buffer + (*pos), sizeof(int) * len, len);
      *pos = *pos + sizeof(int) * len;
      data = x;
      return loc + 2;
    }
    case 1:
    {
      int len;
      get_data((char*)&len, buffer + (*pos), sizeof(int), 1);
      *pos = *pos + sizeof(int);
      Vec<long> *x = new Vec<long>(len);
      long *block_data = x->buffer();
      copy_data((char*)block_data, buffer + (*pos), sizeof(long) * len, len);
      *pos = *pos + sizeof(long) * len;
      data = x;
      return loc + 2;
    }
    case 3:
    {
      int len;
      get_data((char*)&len, buffer + (*pos), sizeof(int), 1);
      *pos = *pos + sizeof(int);
      Vec<double> *x = new Vec<double>(len);
      double *block_data = x->buffer();
      copy_data((char*)block_data, buffer + (*pos), sizeof(double) * len, len);
      *pos = *pos + sizeof(double) * len;
      data = x;
      return loc + 2;
    }
    default:
      info("Unknown Vec: %d", (*encoded_type)[loc + 1]);
    }
  default:
    return loc + 1;
  }
}

void deserialize(void *&data, const char *buffer, size_t len, vector<int> *encoded_type)
{
  int pos = 0;
  deserialize(data, buffer, encoded_type, 0, &pos);
}
