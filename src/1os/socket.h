/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * A simple wrapper around a tcp socket handle. Throws exceptions in
 * case of errors
 *
 * @exception_safe: basic
 * @thread_safe: unknown
 */

#ifndef UPS_SOCKET_H
#define UPS_SOCKET_H

#include "0root/root.h"

#include <stdio.h>
#include <limits.h>

#include "ups/types.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

class Socket
{
  public:
    // Constructor creates an empty socket
    Socket()
      : m_socket(UPS_INVALID_FD) {
    }

    // Destructor closes the socket
    ~Socket() {
      close();
    }

    // Connects to a remote host
    void connect(const char *hostname, uint16_t port, uint32_t timeout_sec);

    // Sends data to the connected server
    void send(const uint8_t *data, size_t len);

    // Receives data from the connected server; blocking!
    void recv(uint8_t *data, size_t len);

    // Closes the connection; no problem if socket was already closed
    void close();

  private:
    ups_socket_t m_socket;
};

} // namespace upscaledb

#endif /* UPS_SOCKET_H */
