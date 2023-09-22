// Copyright (c) 2021 MIT Digital Currency Initiative,
//                    Federal Reserve Bank of Boston
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include "crypto/sha256.h"
#include "parsec/agent/client.hpp"
#include "parsec/broker/impl.hpp"
#include "parsec/directory/impl.hpp"
#include "parsec/runtime_locking_shard/client.hpp"
#include "parsec/ticket_machine/client.hpp"
#include "parsec/util.hpp"

#include <Python.h>
#include <iostream>
#include <random>
#include <string>
#include <thread>

namespace pythoncontracts {
    std::string firefox_key = "firefox";
    std::string firefox
        = "import webbrowser\n"
          "firefox = webbrowser.Mozilla(\"/usr/bin/firefox\")\n"
          "firefox.open(website)\n";

    std::string arbitrary_update_key = "arbitrary_update";
    std::string arbitrary_update = "return (\"Michael's Bank Account\", 100)";

    std::string stash
        = "from Crypto.PublicKey import RSA\n"
          "from hashlib import sha512\n"
          "amount_hash = int.from_bytes(sha512(bytes(str(amount), "
          "'ascii')).digest(), byteorder='big')\n"
          "return (sender_balance - amount, amount_hash, pow(amount_hash, "
          "reciever_pk[1], reciever_pk[0]))\n";
}

auto main(int argc, char** argv) -> int {
    auto log = std::make_shared<cbdc::logging::log>(
        cbdc::logging::log_level::trace);

    auto sha2_impl = SHA256AutoDetect();
    log->info("using sha2: ", sha2_impl);

    if(argc < 2) {
        log->error("Not enough arguments");
        return 1;
    }
    auto cfg = cbdc::parsec::read_config(argc, argv);
    if(!cfg.has_value()) {
        log->error("Error parsing options");
        return 1;
    }

    log->trace("Connecting to shards");
    auto shards = std::vector<
        std::shared_ptr<cbdc::parsec::runtime_locking_shard::interface>>();
    for(const auto& shard_ep : cfg->m_shard_endpoints) {
        auto client = std::make_shared<
            cbdc::parsec::runtime_locking_shard::rpc::client>(
            std::vector<cbdc::network::endpoint_t>{shard_ep});
        if(!client->init()) {
            log->error("Error connecting to shard");
            return 1;
        }
        shards.emplace_back(client);
    }
    log->trace("Connected to shards");

    log->trace("Connecting to ticket machine");
    auto ticketer
        = std::make_shared<cbdc::parsec::ticket_machine::rpc::client>(
            std::vector<cbdc::network::endpoint_t>{
                cfg->m_ticket_machine_endpoints});
    if(!ticketer->init()) {
        log->error("Error connecting to ticket machine");
        return 1;
    }
    log->trace("Connected to ticket machine");

    auto directory
        = std::make_shared<cbdc::parsec::directory::impl>(shards.size());
    auto broker = std::make_shared<cbdc::parsec::broker::impl>(
        std::numeric_limits<size_t>::max(),
        shards,
        ticketer,
        directory,
        log);

    auto pay_contract = cbdc::buffer();
    // std::string contract
    //     = "import webbrowser\n"
    //       "firefox = webbrowser.Mozilla(\"/usr/bin/firefox\")\n"
    //       "firefox.open(website)\n";
    pay_contract.append(pythoncontracts::firefox.c_str(),
                        pythoncontracts::firefox.size());

    auto init_error = std::atomic_bool{false};
    auto init_count = std::atomic<size_t>();

    auto pay_contract_key = cbdc::buffer();
    pay_contract_key.append(pythoncontracts::firefox_key.c_str(),
                            pythoncontracts::firefox_key.size());

    log->info("Inserting pay contract");
    auto ret
        = cbdc::parsec::put_row(broker,
                                pay_contract_key,
                                pay_contract,
                                [&](bool res) {
                                    if(!res) {
                                        init_error = true;
                                    } else {
                                        log->info("Inserted pay contract");
                                        init_count++;
                                    }
                                });
    if(!ret) {
        init_error = true;
    }

    constexpr uint64_t timeout = 30;

    constexpr auto wait_time = std::chrono::seconds(1);
    for(size_t count = 0; init_count < 1 && !init_error && count < timeout;
        count++) {
        std::this_thread::sleep_for(wait_time);
    }
    if(init_error || init_count < 1) {
        log->error("Error adding pay contract");
        return 2;
    }

    pay_contract = cbdc::buffer();
    pay_contract.append(pythoncontracts::arbitrary_update.c_str(),
                        pythoncontracts::arbitrary_update.size());
    pay_contract_key = cbdc::buffer();
    pay_contract_key.append(pythoncontracts::arbitrary_update_key.c_str(),
                            pythoncontracts::arbitrary_update_key.size());

    log->info("Inserting arbitrary pay contract");
    ret = cbdc::parsec::put_row(broker,
                                pay_contract_key,
                                pay_contract,
                                [&](bool res) {
                                    if(!res) {
                                        init_error = true;
                                    } else {
                                        log->info(
                                            "Inserted arbitrary pay contract");
                                        init_count++;
                                    }
                                });
    if(!ret) {
        init_error = true;
    }

    auto agents
        = std::vector<std::shared_ptr<cbdc::parsec::agent::rpc::client>>();
    for(auto& a : cfg->m_agent_endpoints) {
        auto agent = std::make_shared<cbdc::parsec::agent::rpc::client>(
            std::vector<cbdc::network::endpoint_t>{a});
        if(!agent->init()) {
            log->error("Error connecting to agent");
            return 1;
        } else {
            log->trace("Connected to agent");
        }
        agents.emplace_back(agent);
    }
    auto params = cbdc::buffer();
    params.append("python.org", 10);
    auto r = agents[0]->exec(
        pay_contract_key,
        params,
        false,
        [&](cbdc::parsec::agent::interface::exec_return_type res) {
            auto success
                = std::holds_alternative<cbdc::parsec::agent::return_type>(
                    res);
            if(success) {
                log->info("success!");
            }
            log->info("no success :(");
        });
    if(!r) {
        log->error("exec error");
    }

    log->trace("Complete");

    std::string website;
    std::cout << "Enter website name: ";
    std::cin >> website;

    params = cbdc::buffer();
    params.append(website.c_str(), website.length());
    r = agents[0]->exec(
        pay_contract_key,
        params,
        false,
        [&](cbdc::parsec::agent::interface::exec_return_type res) {
            auto success
                = std::holds_alternative<cbdc::parsec::agent::return_type>(
                    res);
            if(success) {
                log->info("success!");
            }
            log->info("no success :(");
        });
    if(!r) {
        log->error("exec error");
    }

    return 0;
}
