// Copyright (c) 2021 MIT Digital Currency Initiative,
//                    Federal Reserve Bank of Boston
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include "broker/tinybroker.hpp"
#include "crypto/sha256.h"
#include "directory/impl.hpp"
#include "format.hpp"
#include "impl.hpp"
#include "runners/evm/format.hpp"
#include "runners/evm/http_server.hpp"
#include "runners/evm/math.hpp"
#include "runners/evm/messages.hpp"
#include "runners/evm/util.hpp"
#include "runners/lua/server.hpp"
#include "runtime_locking_shard/client.hpp"
#include "ticket_machine/client.hpp"
#include "util.hpp"
#include "util/common/logging.hpp"
#include "util/rpc/format.hpp"
#include "util/rpc/tcp_server.hpp"
#include "util/serialization/format.hpp"

#include <csignal>

auto main(int argc, char** argv) -> int {
    auto log = std::make_shared<cbdc::logging::log>(
        cbdc::logging::log_level::trace);

    auto sha2_impl = SHA256AutoDetect();
    log->info("using sha2: ", sha2_impl);

    auto cfg = cbdc::parsec::read_tiny_config(argc, argv);
    if(!cfg.has_value()) {
        log->error("Error parsing options");
        return 1;
    }

    log->set_loglevel(cfg->m_loglevel);

    if(cfg->m_agent_endpoints.size() <= cfg->m_component_id) {
        log->error("No endpoint for component id");
        return 1;
    }

    auto broker
        = std::make_shared<cbdc::parsec::broker::tinybroker>(cfg->m_component_id,
                                                       log);

    if(cfg->m_runner_type == cbdc::parsec::runner_type::evm) {
        if(cfg->m_component_id == 0) {
            auto res
                = cbdc::parsec::agent::runner::mint_initial_accounts(log,
                                                                     broker);
            if(!res) {
                log->error("Error minting initial accounts");
                return 1;
            }
        } else {
            log->info("Not seeding, waiting so role 0 can seed");
            static constexpr auto seeding_time = 10;
            std::this_thread::sleep_for(std::chrono::seconds(seeding_time));
        }
    }

    auto server
        = std::unique_ptr<cbdc::parsec::agent::rpc::server_interface>();

    if(cfg->m_runner_type == cbdc::parsec::runner_type::lua) {
        auto rpc_server = std::make_unique<
            cbdc::rpc::async_tcp_server<cbdc::parsec::agent::rpc::request,
                                        cbdc::parsec::agent::rpc::response>>(
            cfg->m_agent_endpoints[cfg->m_component_id]);
        server = std::make_unique<cbdc::parsec::agent::rpc::server>(
            std::move(rpc_server),
            broker,
            log,
            cfg.value());
    } else if(cfg->m_runner_type == cbdc::parsec::runner_type::evm) {
        auto rpc_server = std::make_unique<cbdc::rpc::json_rpc_http_server>(
            cfg->m_agent_endpoints[cfg->m_component_id],
            true);
        server = std::make_unique<cbdc::parsec::agent::rpc::http_server>(
            std::move(rpc_server),
            broker,
            log,
            cfg.value());
    } else {
        log->error("Unknown runner type");
        return 1;
    }

    if(!server->init()) {
        log->error("Error listening on RPC interface");
        return 1;
    }

    static auto running = std::atomic_bool{true};

    std::signal(SIGINT, [](int /* signal */) {
        running = false;
    });

    log->info("Agent running");

    while(running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    log->info("Shutting down...");

    return 0;
}
