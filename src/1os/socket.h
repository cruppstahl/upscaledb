/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * A simple wrapper around a tcp socket handle. Throws exceptions in
 * case of errors
 *
 * @exception_safe: basic
 * @thread_safe: unknown
 */

#ifndef HAM_SOCKET_H
#define HAM_SOCKET_H

#include "0root/root.h"

#include <stdio.h>
#include <limits.h>

#include "ham/types.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Socket
{
  public:
    // Constructor creates an empty socket
    Socket()
      : m_socket(HAM_INVALID_FD) {
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
    ham_socket_t m_socket;
};

} // namespace hamsterdb

#endif /* HAM_SOCKET_H */
