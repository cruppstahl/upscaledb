/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#ifndef UPS_BENCH_GRAPH_H
#define UPS_BENCH_GRAPH_H

#include <string>
#include <fstream>
#include <stdio.h>

#include <boost/filesystem.hpp>

//
// A class which writes a PNG graph
//
struct Graph
{
  // constructor
  Graph(const char *name)
    : name_(name), latency_file_(0), throughput_file_(0),
      has_lat_inserts_(false), has_lat_finds_(false),
      has_lat_erases_(false), has_lat_commits_(false) {
  }

  // destructor - creates the PNG file
  ~Graph() {
    generate_png();
    if (latency_file_) {
      ::fclose(latency_file_);
      latency_file_ = 0;
    }
    if (throughput_file_) {
      ::fclose(throughput_file_);
      throughput_file_ = 0;
    }
  }

  // add information to the "operations per second" graph
  void add_opspersec_graph(uint64_t time, uint32_t insert, uint32_t find,
                  uint32_t erase, uint32_t commit) {
    if (!throughput_file_) {
      char filename[128];
      ::sprintf(filename, "%s-ops.dat", name_.c_str());
      throughput_file_ = ::fopen(filename, "w");
      if (!throughput_file_) {
        std::cerr << "error writing to file: " << ::strerror(errno)
            << std::endl;
        ::exit(-1);
      }
      ::setvbuf(throughput_file_, NULL, _IOFBF, 2 * 1024 * 1024);
    }
    ::fprintf(throughput_file_, "%lu %u %u %u %u\n", time, insert, find,
                      erase, commit);
  }

  // add latency metrics to the graphs
  void add_latency_metrics(double time, double lat_insert, double lat_find,
                  double lat_erase, double lat_commit, uint32_t page_fetch,
                  uint32_t page_flush) {
    if (!latency_file_) {
      char filename[128];
      ::sprintf(filename, "%s-lat.dat", name_.c_str());
      latency_file_ = ::fopen(filename, "w");
      if (!latency_file_) {
        ::printf("error writing to file: %s\n", ::strerror(errno));
        ::exit(-1);
      }
      ::setvbuf(latency_file_, NULL, _IOFBF, 10 * 1024 * 1024);
    }
    if (lat_insert > 0.0)
      has_lat_inserts_ = true;
    if (lat_erase > 0.0)
      has_lat_erases_ = true;
    if (lat_find > 0.0)
      has_lat_finds_ = true;
    if (lat_commit > 0.0)
      has_lat_commits_ = true;
    ::fprintf(latency_file_, "%f %f %f %f %f %u %u\n", time, lat_insert,
                    lat_find, lat_erase, lat_commit, page_fetch, page_flush);
  }

  // generates a PNG from the accumulated data
  void generate_png() {
    boost::filesystem::remove("graph-lat.png");
    boost::filesystem::remove("graph-ops.png");
    if (latency_file_) {
      ::fflush(latency_file_);

      std::ofstream os;
      os.open("gnuplot-lat");
      os << "reset" << std::endl
         << "set terminal png" << std::endl
         << "set xlabel \"time\"" << std::endl
         << "set ylabel \"latency (thread #1)\"" << std::endl
         << "set style data linespoint" << std::endl
         << "plot \"upscaledb-lat.dat\" using 1:2 title \"insert\"";
      if (has_lat_finds_)
         os << ", \"\" using 1:3 title \"find\"";
      if (has_lat_erases_)
         os << ", \"\" using 1:4 title \"erase\"";
      if (has_lat_commits_)
         os << ", \"\" using 1:5 title \"txn-commit\"";
      os << std::endl;
      os.close();

      int s = ::system("gnuplot gnuplot-lat > graph-lat.png");
      (void) s; // avoid compiler warning
    }

    if (throughput_file_) {
      ::fflush(throughput_file_);

      std::ofstream os;
      os.open("gnuplot-ops");
      os << "reset" << std::endl
         << "set terminal png" << std::endl
         << "set xlabel \"time\"" << std::endl
         << "set ylabel \"operations (all threads)\"" << std::endl
         << "set style data linespoint" << std::endl
         << "plot \"upscaledb-ops.dat\" using 1:2 title \"insert\"";
      if (has_lat_finds_)
         os << ", \"\" using 1:3 title \"find\"";
      if (has_lat_erases_)
         os << ", \"\" using 1:4 title \"erase\"";
      if (has_lat_commits_)
         os << ", \"\" using 1:5 title \"txn-commit\"";
      os << std::endl;
      os.close();

      int s = ::system("gnuplot gnuplot-ops > graph-ops.png");
      (void) s; // avoid compiler warning
    }
  }

  // name: required for the filenames and labels
  std::string name_;

  // file handle for the latency output
  FILE *latency_file_;

  // file handle for the operation-per-second
  FILE *throughput_file_;

  // flags to decide whether certain graphs are printed
  bool has_lat_inserts_;
  bool has_lat_finds_;
  bool has_lat_erases_;
  bool has_lat_commits_;
};

#endif /* UPS_BENCH_GRAPH_H */
