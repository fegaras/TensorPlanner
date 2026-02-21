module {
  memref.global "private" @a_smem_global : memref<64x17xf32, 3>
  memref.global "private" @b_smem_global : memref<16x65xf32, 3>
  func.func @matmul(%arg0 : memref<4096x4096xf32>, %arg1 : memref<4096x4096xf32>, %arg2 : memref<4096x4096xf32>)
      -> memref<4096x4096xf32>
  {
    // Grid : (4096/64) x (4096/64) = 64 x 64 blocks
    // Block: (64/8)   x (64/8)    = 8  x 8  threads = 64 threads
    %c1    = arith.constant 1  : index
    %c8    = arith.constant 8  : index
    %c64   = arith.constant 64 : index
    %c16   = arith.constant 16 : index

    gpu.launch
        blocks (%bx, %by, %bz) in (%gbx = %c64, %gby = %c64, %gbz = %c1)
        threads(%tx, %ty, %tz) in (%tbx = %c8,  %tby = %c8,  %tbz = %c1)
    {
      // Each block covers a (64 x 64) tile of C
      %row_start = arith.muli %bx, %c64 : index   // first row this block owns
      %col_start = arith.muli %by, %c64 : index   // first col this block owns

      // Thread-level tile origin (within block)
      // Each thread owns an (8 x 8) sub-tile
      %thread_row = arith.muli %tx, %c8 : index   // row offset within block tile
      %thread_col = arith.muli %ty, %c8 : index   // col offset within block tile

      // Allocate shared memory locally per block
      // A tile: (BM x BK) = 64 x 16, padded +1 col to avoid bank conflicts
      // B tile: (BK x BN) = 16 x 64, padded +1 col
      %a_smem = memref.get_global @a_smem_global : memref<64x17xf32, 3>
      %b_smem = memref.get_global @b_smem_global : memref<16x65xf32, 3>

      // ── Thread-local accumulator for (TM x TN) = 8x8 output
      %c_local = memref.alloca() : memref<8x8xf32>

      // Zero out accumulator
      affine.for %ii = 0 to 8 {
        affine.for %jj = 0 to 8 {
          %zero = arith.constant 0.0 : f32
          affine.store %zero, %c_local[%ii, %jj] : memref<8x8xf32>
        }
      }

      // Outer loop over K tiles
      affine.for %k = 0 to 4096 step 16 {

        // Cooperative load: all 64 threads load A tile (64x16)
        // Thread (tx, ty) loads rows [tx*1 .. tx*1] and cols determined by ty
        // Simple scheme: linearize thread id and stride through the tile
        %0 = arith.muli %ty, %c8 : index
        %tid_linear = arith.addi %tx, %0 : index   // 0..63

        // Each thread loads 64*16/64 = 16 elements of A
        affine.for %load_idx = 0 to 16 {
          %1 = arith.muli %tid_linear, %c16 : index
          %elem_idx  = arith.addi %1, %load_idx : index
          %a_row_loc = arith.divui %elem_idx, %c16 : index   // 0..63
          %a_col_loc = arith.remui %elem_idx, %c16 : index   // 0..15
          %a_row_gbl = arith.addi %row_start, %a_row_loc : index
          %a_col_gbl = arith.addi %k,         %a_col_loc : index
          %a_val     = memref.load %arg0[%a_row_gbl, %a_col_gbl]
                           : memref<4096x4096xf32>
          memref.store %a_val, %a_smem[%a_row_loc, %a_col_loc]
                           : memref<64x17xf32, 3>
        }

        affine.for %load_idx = 0 to 16 {
          %1 = arith.muli %tid_linear, %c16 : index
          %elem_idx  = arith.addi %1, %load_idx : index
          %b_row_loc = arith.divui %elem_idx, %c64 : index   // 0..15
          %b_col_loc = arith.remui %elem_idx, %c64 : index   // 0..63
          %b_row_gbl = arith.addi %k,         %b_row_loc : index
          %b_col_gbl = arith.addi %col_start, %b_col_loc : index
          %b_val     = memref.load %arg1[%b_row_gbl, %b_col_gbl]
                           : memref<4096x4096xf32>
          memref.store %b_val, %b_smem[%b_row_loc, %b_col_loc]
                           : memref<16x65xf32, 3>
        }
        gpu.barrier

        affine.for %kk = 0 to 16 {
          affine.for %ii = 0 to 8 {
            affine.for %jj = 0 to 8 {
              %a_row = arith.addi %thread_row, %ii : index
              %b_col = arith.addi %thread_col, %jj : index

              %a_val   = memref.load %a_smem[%a_row, %kk]
                             : memref<64x17xf32, 3>
              %b_val   = memref.load %b_smem[%kk, %b_col]
                             : memref<16x65xf32, 3>
              %acc_old = memref.load %c_local[%ii, %jj]
                             : memref<8x8xf32>
              %prod    = arith.mulf %a_val, %b_val : f32
              %acc_new = arith.addf %acc_old, %prod : f32
              memref.store %acc_new, %c_local[%ii, %jj]
                             : memref<8x8xf32>
            }
          }
        }
        gpu.barrier
      }

      affine.for %ii = 0 to 8 {
        affine.for %jj = 0 to 8 {
          %1 = arith.addi %row_start, %thread_row : index
          %2 = arith.addi %col_start, %thread_col : index
          %c_row = arith.addi %1, %ii : index
          %c_col = arith.addi %2, %jj : index
          %val = memref.load %c_local[%ii, %jj] : memref<8x8xf32>
          memref.store %val, %arg2[%c_row, %c_col] : memref<4096x4096xf32>
        }
      }

      gpu.terminator
    }

    return %arg2 : memref<4096x4096xf32>
  }
}