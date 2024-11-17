// Copyright (c) 2021 MIT Digital Currency Initiative,
//                    Federal Reserve Bank of Boston
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include "tcp_socket.hpp"

#include <array>
#include <cstring>
#include <unistd.h>
#include <sys/uio.h> // for iovec, writev
#include <netinet/tcp.h> // for TCP_NODELAY, TCP_QUICKACK

namespace cbdc::network {
    auto tcp_socket::connect(const endpoint_t& ep) -> bool {
        return connect(ep.first, ep.second);
    }

    auto tcp_socket::connect(const ip_address& remote_address,
                             port_number_t remote_port) -> bool {
        m_addr = remote_address;
        m_port = remote_port;
        auto res0 = get_addrinfo(remote_address, remote_port);
        if(!res0) {
            return false;
        }

        for(auto* res = res0.get(); res != nullptr; res = res->ai_next) {
            if(!create_socket(res->ai_family,
                              res->ai_socktype,
                              res->ai_protocol)) {
                continue;
            }

            if(::connect(m_sock_fd, res->ai_addr, res->ai_addrlen) != 0) {
                ::close(m_sock_fd);
                m_sock_fd = -1;
                continue;
            }

            static constexpr int one = 1;
            setsockopt(m_sock_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); // sending side. needed?
#ifdef __linux__
            setsockopt(m_sock_fd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one)); // receiving side
#endif
            break;
        }

        m_connected = m_sock_fd != -1;

        return m_connected;
    }

    void tcp_socket::disconnect() {
        if(m_sock_fd != -1) {
            m_connected = false;
            shutdown(m_sock_fd, SHUT_RDWR);
            close(m_sock_fd);
            m_sock_fd = -1;
        }
    }

    tcp_socket::~tcp_socket() {
        disconnect();
    }

    auto tcp_socket::send(const buffer& pkt) const -> bool {
        const auto sz_val = static_cast<uint64_t>(pkt.size());
        std::array<std::byte, sizeof(sz_val)> sz_arr{};
        std::memcpy(sz_arr.data(), &sz_val, sizeof(sz_val));

        struct iovec iov[2];
        iov[0].iov_base = sz_arr.data();
        iov[0].iov_len = sizeof(sz_val);
        iov[1].iov_base = const_cast<void*>(pkt.data());
        iov[1].iov_len = pkt.size();

        size_t total_bytes = sizeof(sz_val) + pkt.size();
        size_t total_written = 0;

        while (total_written < total_bytes) {
            ssize_t n = writev(m_sock_fd, iov, 2);
            if (n <= 0) {
                return false;
            }
            total_written += static_cast<size_t>(n);

            if (n <= static_cast<ssize_t>(sizeof(sz_val))) {
                iov[0].iov_base = static_cast<std::byte*>(iov[0].iov_base) + n;
                iov[0].iov_len -= n;
            } else {
                n -= iov[0].iov_len;
                iov[0].iov_len = 0;
                iov[1].iov_base = static_cast<std::byte*>(iov[1].iov_base) + n;
                iov[1].iov_len -= n;
            }
        }

        return true;
    }

    auto tcp_socket::receive(buffer& pkt) const -> bool {
        static constexpr int one = 1;
        // apparently TCP_QUICKACK needs to be re-set after each read (incurring a syscall...)
        // cf. https://github.com/netty/netty/issues/13610

        uint64_t pkt_sz{};
        std::array<std::byte, sizeof(pkt_sz)> sz_buf{};
        uint64_t total_read{0};
        while(total_read != sz_buf.size()) {
            auto n = recv(m_sock_fd,
                          &sz_buf.at(total_read),
                          sizeof(pkt_sz) - total_read,
                          0);
#ifdef __linux__
            setsockopt(m_sock_fd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one)); // receiving side
#endif
            if(n <= 0) {
                return false;
            }
            total_read += static_cast<uint64_t>(n);
        }
        std::memcpy(&pkt_sz, sz_buf.data(), sizeof(pkt_sz));

        pkt.resize(pkt_sz);

        total_read = 0;
        while(total_read < pkt_sz) {
            auto n = recv(m_sock_fd,
                          pkt.data_at(total_read),
                          pkt_sz - total_read,
                          MSG_WAITALL);
#ifdef __linux__
            setsockopt(m_sock_fd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one)); // receiving side
#endif
            if(n <= 0) {
                return false;
            }

            total_read += static_cast<uint64_t>(n);
        }

        return true;
    }

    auto tcp_socket::reconnect() -> bool {
        disconnect();
        if(!m_addr) {
            return false;
        }
        return connect(*m_addr, m_port);
    }

    auto tcp_socket::connected() const -> bool {
        return m_connected;
    }
}
