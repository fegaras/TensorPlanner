import subprocess
import sys

from mlir.ir import Context, Module
from mlir.passmanager import PassManager

# Converts MLIR to PTX code.
def convert_mlir_to_ptx(mlir_module_str: str, chip_type="sm_80"):
    with Context():
        # Parse the input MLIR module
        module = Module.parse(mlir_module_str)
        if(module is None or module.operation is None):
            return None
        if(len(module.operation.regions) == 0 or len(module.operation.regions[0].blocks) == 0
           or len(module.operation.regions[0].blocks[0].operations) < 4):
            return None
        # Extract the GPU operations from the GPU module from the MLIR module
        gpu_operations = extract_gpu_operations(module)
        # Generate PTX from the GPU operations
        ptx = generate_ptx(str(gpu_operations), chip_type)
    return ptx

# extract the GPU module operations from input module
def extract_gpu_operations(module: Module) -> Module:
    try:
        num_ops = len(module.operation.regions[0].blocks[0].operations)
        gpu_module_ops = ""
        gpu_module_ops += str(module.operation.regions[0].blocks[0].operations[0]) + "\n"
        gpu_module_ops += str(module.operation.regions[0].blocks[0].operations[1]) + "\n"
        for i in range(3,num_ops,2):
            gpu_module = module.operation.regions[0].blocks[0].operations[i]
            gpu_module_ops += str(gpu_module.regions[0].blocks[0].operations[0]) + "\n"
        # Create a new module from the GPU module operations
        gpu_ops_module = Module.parse(gpu_module_ops)
        return gpu_ops_module
    except (IndexError, AttributeError) as e:
        raise RuntimeError(f"Failed to extract GPU module operations: {e}") from e

# Generates PTX from an MLIR GPU module string
def generate_ptx(gpu_module_str, chip_type="sm_80"):
    # Convert MLIR to LLVM IR using mlir-translate
    llvm_ir_output = subprocess.run(
        ["mlir-translate", "--mlir-to-llvmir", "-"],
        input=gpu_module_str,
        capture_output=True,
        text=True,
    )

    if llvm_ir_output.returncode != 0:
        print("Error generating LLVM IR:")
        print(llvm_ir_output.stderr)
        return None

    llvm_ir = llvm_ir_output.stdout
    # Convert LLVM IR to PTX using llc
    ptx_output = subprocess.run(
        ["llc", "-march=nvptx64", f"-mcpu={chip_type}", "-"],
        input=llvm_ir,
        capture_output=True,
        text=True,
    )
    if ptx_output.returncode != 0:
        print("Error generating PTX:")
        print(ptx_output.stderr)
        return None
    return ptx_output.stdout

if(len(sys.argv) != 3):
    print("Usage: python extract_gpu_kernel.py <input_mlir_file> <output_ptx_file>")
    sys.exit(1)
input_mlir = sys.argv[1]
output_ptx = sys.argv[2]
ptx_code = convert_mlir_to_ptx(open(input_mlir).read())
if(ptx_code is not None):
    with open(output_ptx, "w") as f:
        f.write(ptx_code)

