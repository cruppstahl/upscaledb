/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef DATASOURCE_H__
#define DATASOURCE_H__

#include <vector>

//
// abstract base class for a data source - generates test data
//
class Datasource
{
  public:
    // virtual destructor - can be overwritten
    virtual ~Datasource() {
    }

    // returns the next piece of data
    virtual void get_next(std::vector<uint8_t> &vec) = 0;
};

#endif /* DATASOURCE_H__ */

