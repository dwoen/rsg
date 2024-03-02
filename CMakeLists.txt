cmake_minimum_required(VERSION 3.13)
cmake_policy(SET CMP0076 NEW)

project(page-rank)

find_package(TAPA REQUIRED)
find_package(FRT REQUIRED)

add_library(page-rank-h INTERFACE)
target_sources(page-rank-h INTERFACE page-rank.h)

add_library(page_rank_host)
target_include_directories(page_rank_host PRIVATE .)
target_sources(page_rank_host PRIVATE nxgraph.hpp page-rank-host.cpp)
target_link_libraries(page_rank_host PUBLIC page-rank-h tapa::tapa)

add_executable(page-rank)
target_sources(page-rank PRIVATE page-rank.cpp)
target_link_libraries(
  page-rank
  PUBLIC page-rank-h page_rank_host
  PRIVATE gflags)

add_tapa_target(
  page-rank-hw-xo
  INPUT page-rank.cpp
  TOP PageRank
  PLATFORM xilinx_u280_xdma_201920_3
  DIRECTIVE ${CMAKE_CURRENT_SOURCE_DIR}/directive.json
  CONSTRAINT ${CMAKE_CURRENT_BINARY_DIR}/constraint.tcl
  CLOCK_PERIOD 4.000)

add_xocc_hw_link_targets(
  ${CMAKE_CURRENT_BINARY_DIR}
  --sp=PageRank.edges_0:HBM[0]
  --sp=PageRank.edges_1:HBM[3]
  --sp=PageRank.edges_2:HBM[6]
  --sp=PageRank.edges_3:HBM[9]
  --sp=PageRank.edges_4:HBM[12]
  --sp=PageRank.edges_5:HBM[15]
  --sp=PageRank.edges_6:HBM[18]
  --sp=PageRank.edges_7:HBM[21]
  --sp=PageRank.updates_0:HBM[2]
  --sp=PageRank.updates_1:HBM[5]
  --sp=PageRank.updates_2:HBM[8]
  --sp=PageRank.updates_3:HBM[11]
  --sp=PageRank.updates_4:HBM[14]
  --sp=PageRank.updates_5:HBM[17]
  --sp=PageRank.updates_6:HBM[20]
  --sp=PageRank.updates_7:HBM[23]
  --sp=PageRank.metadata:PLRAM[0]
  --sp=PageRank.degrees:HBM[24]
  --sp=PageRank.rankings:HBM[26]
  --sp=PageRank.tmps:HBM[27]
  --remote_ip_cache=$ENV{HOME}/.remote_ip_cache
  --kernel_frequency=250
  --vivado.prop=run.impl_1.STRATEGY=Performance_EarlyBlockPlacement
  --vivado.prop=run.impl_1.STEPS.OPT_DESIGN.TCL.PRE=${CMAKE_CURRENT_BINARY_DIR}/constraint.tcl
  INPUT page-rank-hw-xo
  HW_EMU_XCLBIN hw_emu_xclbin_target
  HW_XCLBIN hw_xclbin_target)

enable_testing()

file(
  DOWNLOAD "https://snap.stanford.edu/data/facebook_combined.txt.gz"
  ${CMAKE_CURRENT_BINARY_DIR}/facebook.txt.gz
  EXPECTED_HASH
    "SHA512=54b65fcdd41cad3e96e52f7ca60f326bea117cc8997ad4ec1831053fafa067fae9e728916b81a1fcf41a24de19e416b78bbfb14a3824c6e2bf67704dd7f89ad3"
)
execute_process(
  INPUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/facebook.txt.gz
  COMMAND gzip -cd
  OUTPUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/facebook.txt)
add_test(NAME page-rank COMMAND page-rank
                                ${CMAKE_CURRENT_BINARY_DIR}/facebook.txt 512)

add_custom_target(
  page-rank-cosim
  COMMAND $<TARGET_FILE:page-rank> ${CMAKE_CURRENT_SOURCE_DIR}/graph.txt
          --bitstream=$<TARGET_PROPERTY:${hw_emu_xclbin_target},FILE_NAME>
  DEPENDS page-rank ${hw_emu_xclbin_target}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_custom_target(
  page-rank-hw
  COMMAND $<TARGET_FILE:page-rank> ${CMAKE_CURRENT_BINARY_DIR}/facebook.txt
          --bitstream=$<TARGET_PROPERTY:${hw_xclbin_target},FILE_NAME>
  DEPENDS page-rank ${hw_xclbin_target}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_test(NAME page-rank-cosim
         COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target
                 page-rank-cosim)
