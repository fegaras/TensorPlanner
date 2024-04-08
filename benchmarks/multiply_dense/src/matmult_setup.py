from setuptools import Extension, setup
from Cython.Build import cythonize
import numpy as np

ext_modules = [
    Extension(
        "matmult",
        ["matmult.pyx"],
        extra_compile_args=['-fopenmp'],
        extra_link_args=['-fopenmp'],
    )
]

setup(
    name='matmult-parallel',
    ext_modules=cythonize(ext_modules),
    include_dirs=[np.get_include()],
)