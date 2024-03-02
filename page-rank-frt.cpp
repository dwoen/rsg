#include <stdlib.h>

#include <algorithm>

#include <frt.h>
#include <tapa.h>

#include "page-rank.h"

void PageRank(Pid num_partitions, tapa::mmap<uint64_t> metadata,
              tapa::async_mmap<FloatVec> degrees,
              tapa::async_mmap<FloatVec> rankings,
              tapa::async_mmap<FloatVec> tmps,
              tapa::async_mmaps<EdgeVec, kNumPes> edges,
              tapa::async_mmaps<UpdateVec, kNumPes> updates) {
  auto instance = fpga::Instance(getenv("BITSTREAM"));
  auto metadata_arg = fpga::ReadWrite(metadata.get(), metadata.size());
  auto degrees_arg = fpga::ReadWrite(degrees.get(), degrees.size());
  auto rankings_arg = fpga::ReadWrite(rankings.get(), rankings.size());
  auto tmps_arg = fpga::ReadWrite(tmps.get(), tmps.size());
  int arg_idx = 0;
  instance.SetArg(arg_idx++, num_partitions);
  instance.AllocBuf(arg_idx, metadata_arg);
  instance.SetArg(arg_idx++, metadata_arg);
  instance.AllocBuf(arg_idx, degrees_arg);
  instance.SetArg(arg_idx++, degrees_arg);
  instance.AllocBuf(arg_idx, rankings_arg);
  instance.SetArg(arg_idx++, rankings_arg);
  instance.AllocBuf(arg_idx, tmps_arg);
  instance.SetArg(arg_idx++, tmps_arg);
  for (int i = 0; i < kNumPes; ++i) {
    auto edges_arg = fpga::WriteOnly(edges[i].get(), edges[i].size());
    auto updates_arg = fpga::ReadOnly(updates[i].get(), updates[i].size());
    instance.AllocBuf(arg_idx + i, edges_arg);
    instance.SetArg(arg_idx + i, edges_arg);
    instance.AllocBuf(arg_idx + kNumPes + i, updates_arg);
    instance.SetArg(arg_idx + kNumPes + i, updates_arg);
  }

  instance.WriteToDevice();
  instance.Exec();
  instance.ReadFromDevice();
  instance.Finish();

  LOG(INFO) << "kernel time: " << instance.ComputeTimeSeconds() * 1e3 << " ms";
}
