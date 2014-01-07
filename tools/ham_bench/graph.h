/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef GRAPH_H__
#define GRAPH_H__

#include <string>
#include <fstream>
#include <stdio.h>

#include <boost/filesystem.hpp>

//
// A class which writes a PNG graph
//
class Graph
{
  public:
    // constructor
    Graph(const char *name)
      : m_name(name), m_latency_file(0), m_opspersec_file(0),
        m_has_lat_inserts(false), m_has_lat_finds(false),
        m_has_lat_erases(false), m_has_lat_commits(false) {
    }

    // destructor - creates the PNG file
    virtual ~Graph() {
      generate_png();
      if (m_latency_file) {
        fclose(m_latency_file);
        m_latency_file = 0;
      }
      if (m_opspersec_file) {
        fclose(m_opspersec_file);
        m_opspersec_file = 0;
      }
    }

    // add information to the "operations per second" graph
    void add_opspersec_graph(uint64_t time, uint32_t insert, uint32_t find,
                    uint32_t erase, uint32_t commit) {
      if (!m_opspersec_file) {
        char filename[128];
        sprintf(filename, "%s-ops.dat", m_name.c_str());
        m_opspersec_file = fopen(filename, "w");
        if (!m_opspersec_file) {
          printf("error writing to file: %s\n", strerror(errno));
          exit(-1);
        }
        setvbuf(m_opspersec_file, NULL, _IOFBF, 2 * 1024 * 1024);
      }
      fprintf(m_opspersec_file, "%lu %u %u %u %u\n", time, insert,
                      find, erase, commit);
    }

    // add latency metrics to the graphs
    void add_latency_metrics(double time, double lat_insert, double lat_find,
                    double lat_erase, double lat_commit, uint32_t page_fetch,
                    uint32_t page_flush) {
      if (!m_latency_file) {
        char filename[128];
        sprintf(filename, "%s-lat.dat", m_name.c_str());
        m_latency_file = fopen(filename, "w");
        if (!m_latency_file) {
          printf("error writing to file: %s\n", strerror(errno));
          exit(-1);
        }
        setvbuf(m_latency_file, NULL, _IOFBF, 10 * 1024 * 1024);
      }
      if (lat_insert > 0.0)
        m_has_lat_inserts = true;
      if (lat_erase > 0.0)
        m_has_lat_erases = true;
      if (lat_find > 0.0)
        m_has_lat_finds = true;
      if (lat_commit > 0.0)
        m_has_lat_commits = true;
      fprintf(m_latency_file, "%f %f %f %f %f %u %u\n", time, lat_insert,
                      lat_find, lat_erase, lat_commit, page_fetch, page_flush);
    }

    // generates a PNG from the accumulated data
    void generate_png() {
      boost::filesystem::remove("graph-lat.png");
      boost::filesystem::remove("graph-ops.png");
      if (m_latency_file) {
        fflush(m_latency_file);

        std::ofstream os;
        os.open("gnuplot-lat");
        os << "reset" << std::endl
           << "set terminal png" << std::endl
           << "set xlabel \"time\"" << std::endl
           << "set ylabel \"latency (thread #1)\"" << std::endl
           << "set style data linespoint" << std::endl
           << "plot \"hamsterdb-lat.dat\" using 1:2 title \"insert\"";
        if (m_has_lat_finds)
           os << ", \"\" using 1:3 title \"find\"";
        if (m_has_lat_erases)
           os << ", \"\" using 1:4 title \"erase\"";
        if (m_has_lat_commits)
           os << ", \"\" using 1:5 title \"txn-commit\"";
        os << std::endl;
        os.close();

		int foo = ::system("gnuplot gnuplot-lat > graph-lat.png");
        (void)foo;
      }

      if (m_opspersec_file) {
        fflush(m_opspersec_file);

        std::ofstream os;
        os.open("gnuplot-ops");
        os << "reset" << std::endl
           << "set terminal png" << std::endl
           << "set xlabel \"time\"" << std::endl
           << "set ylabel \"operations (all threads)\"" << std::endl
           << "set style data linespoint" << std::endl
           << "plot \"hamsterdb-ops.dat\" using 1:2 title \"insert\"";
        if (m_has_lat_finds)
           os << ", \"\" using 1:3 title \"find\"";
        if (m_has_lat_erases)
           os << ", \"\" using 1:4 title \"erase\"";
        if (m_has_lat_commits)
           os << ", \"\" using 1:5 title \"txn-commit\"";
        os << std::endl;
        os.close();

		int foo = ::system("gnuplot gnuplot-ops > graph-ops.png");
        (void)foo;
      }
    }

  private:
    // name: required for the filenames and labels
    std::string m_name;

    // file handle for the latency output
    FILE *m_latency_file;

    // file handle for the operation-per-second
    FILE *m_opspersec_file;

    // flags to decide whether certain graphs are printed
    bool m_has_lat_inserts;
    bool m_has_lat_finds;
    bool m_has_lat_erases;
    bool m_has_lat_commits;
};

#endif /* GRAPH_H__ */

