#map0 = affine_map<(d0) -> (d0)>
    #map1 = affine_map<(d0) -> (d0 + 128)>
    #map2 = affine_map<(d0) -> (d0 + 8)>
    #map3 = affine_map<(d0,d1) -> (d0 + d1)>
    module {
    memref.global "private" @a_smem_global : memref<8x128xf32, 3>
    memref.global "private" @b_smem_global : memref<128x8xf32, 3>
    

    func.func @_v_67(%arg2 : memref<4096x4096xf32>, %arg1 : memref<4096x4096xf32>, %arg0 : memref<4096x4096xf32>) -> memref<4096x4096xf32> {
    
    %c1 = arith.constant 1 : index
    %c2 = arith.constant 8 : index
    %c512 = arith.constant 512 : index
    gpu.launch blocks(%bx, %by, %bz) in (%grid_x = %c512, %grid_y = %c1, %grid_z = %c1)
                threads(%tx, %ty, %tz) in (%block_x = %c512, %block_y = %c1, %block_z = %c1) {
      %i = arith.muli %c2, %bx : index
      %j = arith.muli %c2, %tx : index
      %a_smem = memref.get_global @a_smem_global : memref<8x128xf32, 3>
      %b_smem = memref.get_global @b_smem_global : memref<128x8xf32, 3>
      affine.for %k = 0 to 4096 step 128 {
        affine.for %copyii = #map0(%i) to #map2(%i) {
          affine.for %copykk = #map0(%k) to #map1(%k) {
            %0 = affine.load %arg0[%copyii, %copykk] : memref<4096x4096xf32>
            affine.store %0, %a_smem[%copyii - %i, %copykk - %k] : memref<8x128xf32, 3>
          }
        }
        affine.for %copykk = #map0(%k) to #map1(%k) {
          affine.for %copyjj = #map0(%j) to #map2(%j) {
            %0 = affine.load %arg1[%copykk, %copyjj] : memref<4096x4096xf32>
            affine.store %0, %b_smem[%copykk - %k, %copyjj - %j] : memref<128x8xf32, 3>
          }
        }
        affine.for %ii = 0 to 8 {
          affine.for %jj = 0 to 8 {
            %0 = affine.apply #map3(%i, %ii)
            %1 = affine.apply #map3(%j, %jj)
            %2 = affine.load %arg2[%0, %1] : memref<4096x4096xf32>
            %res = affine.for %kk = 0 to 128 iter_args(%accum = %2) -> (f32) {
              %3 = affine.load %a_smem[%ii, %kk] : memref<8x128xf32, 3>
              %4 = affine.load %b_smem[%kk, %jj] : memref<128x8xf32, 3>
              %5 = arith.mulf %3, %4 : f32
              %6 = arith.addf %accum, %5 : f32
              affine.yield %6 : f32
            }
            affine.store %res, %arg2[%0, %1] : memref<4096x4096xf32>
          }
        }
      }
      gpu.terminator
    }
    

    return %arg2 : memref<4096x4096xf32>
}
    
}

