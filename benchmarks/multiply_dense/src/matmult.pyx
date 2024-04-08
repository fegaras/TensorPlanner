cimport cython
import numpy as np
cimport numpy as np
from cython.parallel import prange

def create_matrix(int rows, int cols):
    return np.zeros((rows, cols), dtype=np.float32)

@cython.boundscheck(False)
@cython.wraparound(False)
def add_matrix():
    cdef int n = 1000
    cdef np.ndarray[np.float32_t, ndim=2] A = 0.1*np.ones((n, n), dtype=np.float32)
    cdef np.ndarray[np.float32_t, ndim=2] B = 1.3*np.ones((n, n), dtype=np.float32)
    cdef np.ndarray[np.float32_t, ndim=2] C = np.zeros((n, n), dtype=np.float32)

    cdef float[:, :] A_view = A
    cdef float[:, :] B_view = B
    cdef float[:, :] C_view = C

    cdef Py_ssize_t i, j, k

    for i in prange(n, nogil=True):
        for j in range(n):
            C_view[i, j] = A_view[i, j] + B_view[i, j]

@cython.boundscheck(False)
@cython.wraparound(False)
def multiply_matrix():
    cdef int n = 1000
    cdef np.ndarray[np.float32_t, ndim=2] A = 0.1*np.ones((n, n), dtype=np.float32)
    cdef np.ndarray[np.float32_t, ndim=2] B = 1.3*np.ones((n, n), dtype=np.float32)
    cdef np.ndarray[np.float32_t, ndim=2] C = np.zeros((n, n), dtype=np.float32)

    cdef float[:, :] A_view = A
    cdef float[:, :] B_view = B
    cdef float[:, :] C_view = C

    cdef Py_ssize_t i, j, k

    for i in prange(n, nogil=True):
        for j in range(n):
            for k in range(n):
                C_view[i, j] += A_view[i, k] * B_view[k, j]

