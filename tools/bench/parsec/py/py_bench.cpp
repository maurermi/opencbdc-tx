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
    /*
    , = delimiter between values
    | = delimiter between header region
    return_types | return_args | input_args | function
    */

    std::string firefox_key = "firefox";
    std::string firefox
        = "s|website1,|website1,website2,|import webbrowser\n"
          "firefox = webbrowser.Mozilla(\"/usr/bin/firefox\")\n"
          "firefox.open(website1)\n"
          "print(website2)\n"
          "firefox.open(website2)\n"
          "website1 = 100";

    std::string arbitrary_update_key = "arbitrary_update";
    std::string arbitrary_update
        = "account = \"0x3B2F51dad57e4160fd51DdB9A502c320B3f6363f\"\n"
          "new_balance = 100\n";

    std::string mult_key = "multiply";
    std::string multiply = "c = a * b\n"
                           "return c";

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
                                        log->info("Inserted pay contract", pay_contract.c_str());
                                        init_count++;
                                    }
                                });
    if(!ret) {
        init_error = true;
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // std::string michael = "michael";
    // auto k = cbdc::buffer();
    // k.append(michael.c_str(), michael.size());
    // auto v = cbdc::buffer();
    // v.append("abcde", 5);
    // auto f = cbdc::parsec::put_row(broker, k, v, [&](bool res) {
    //     if(!res) {
    //         log->info("Did not insert michael");
    //     } else {
    //         log->info("Inserted michael");
    //     }
    // });
    // if(!f) {
    //     init_error = true;
    // }

    // std::this_thread::sleep_for(std::chrono::seconds(10));

    // auto return_value = cbdc::buffer();
    // ret = cbdc::parsec::get_row(broker, k, [&](cbdc::parsec::broker::interface::try_lock_return_type res) {
    //     if(std::holds_alternative<
    //            cbdc::parsec::runtime_locking_shard::value_type>(res)) {
    //         auto r = std::get<cbdc::parsec::runtime_locking_shard::value_type>(res);
    //         log->trace("Found this (callback):", r.c_str());
    //         return_value = r;
    //     }
    //     else {
    //         log->error("get row callback recieved error");
    //     }
    // });
    // std::this_thread::sleep_for(std::chrono::seconds(10));
    // log->trace("Found this:", return_value.c_str());

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
    // init_count = 0;

    // pay_contract = cbdc::buffer();
    // pay_contract.append(pythoncontracts::arbitrary_update.c_str(),
    //                     pythoncontracts::arbitrary_update.size());
    // pay_contract_key = cbdc::buffer();
    // pay_contract_key.append(pythoncontracts::arbitrary_update_key.c_str(),
    //                         pythoncontracts::arbitrary_update_key.size());

    // log->info("Inserting arbitrary pay contract");
    // ret = cbdc::parsec::put_row(broker,
    //                             pay_contract_key,
    //                             pay_contract,
    //                             [&](bool res) {
    //                                 if(!res) {
    //                                     init_error = true;
    //                                 } else {
    //                                     log->info(
    //                                         "Inserted arbitrary pay
    //                                         contract");
    //                                     init_count++;
    //                                 }
    //                             });
    // if(!ret) {
    //     init_error = true;
    // }

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

    std::string website = "bing.com";
    // std::cout << "Enter website name: ";
    // std::cin >> website;

    params.append(website.c_str(), website.length() + 1);
    params.append("python.org\0", 11);
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
    // std::this_thread::sleep_for(std::chrono::seconds(20));
    // log->trace("doing it again");
    // r = agents[0]->exec(
    //     pay_contract_key,
    //     params,
    //     false,
    //     [&](cbdc::parsec::agent::interface::exec_return_type res) {
    //         auto success
    //             = std::holds_alternative<cbdc::parsec::agent::return_type>(
    //                 res);
    //         if(success) {
    //             log->info("success!");
    //         }
    //         log->info("no success :(");
    //     });
    // if(!r) {
    //     log->error("exec error");
    // }
    // cbdc::buffer result = get_value_at(michael);

    // make some accounts

    // params = cbdc::buffer();
    // pay_contract = cbdc::buffer();
    // pay_contract.append(pythoncontracts::arbitrary_update.c_str(),
    //                     pythoncontracts::arbitrary_update.size());
    // pay_contract_key = cbdc::buffer();
    // pay_contract_key.append(pythoncontracts::arbitrary_update_key.c_str(),
    //                         pythoncontracts::arbitrary_update_key.size());
    // log->info(pythoncontracts::arbitrary_update.c_str());
    // log->info(pythoncontracts::arbitrary_update_key.c_str());
    // log->info("Inserting arbitrary pay contract");
    // ret = cbdc::parsec::put_row(broker,
    //                             pay_contract_key,
    //                             pay_contract,
    //                             [&](bool res) {
    //                                 if(!res) {
    //                                     init_error = true;
    //                                 } else {
    //                                     log->info(
    //                                         "Inserted arbitrary pay
    //                                         contract");
    //                                     init_count++;
    //                                 }
    //                             });
    // if(!ret) {
    //     init_error = true;
    //     log->error("init error");
    // }
    // for(size_t count = 0; init_count < 1 && !init_error && count < timeout;
    //     count++) {
    //     std::this_thread::sleep_for(wait_time);
    // }
    // if(init_error || init_count < 1) {
    //     log->error("Error adding pay contract");
    //     return 2;
    // }

    // log->info("calling exec");
    // auto r = agents[0]->exec(
    //     pay_contract_key,
    //     params,
    //     false,
    //     [&](cbdc::parsec::agent::interface::exec_return_type res) {
    //         auto success
    //             = std::holds_alternative<cbdc::parsec::agent::return_type>(
    //                 res);
    //         log->trace("measuring success");
    //         if(success) {
    //             auto updates =
    //             std::get<cbdc::parsec::agent::return_type>(res); auto buf =
    //             cbdc::buffer();
    //             buf.append("0x3B2F51dad57e4160fd51DdB9A502c320B3f6363f",
    //             43); auto it = updates.find(buf); log->trace("finding");
    //             assert(it != updates.end());
    //             log->trace("success 238");
    //         } else {
    //             log->trace("error");
    //         }
    //     });
    // if(!r) {
    //     log->error("exec error");
    // }
    std::this_thread::sleep_for(std::chrono::seconds(15));
    auto k = cbdc::buffer();
    k.append("some key", 8);
    auto return_value = cbdc::buffer();
    ret = cbdc::parsec::get_row(broker, k, [&](cbdc::parsec::broker::interface::try_lock_return_type res) {
        if(std::holds_alternative<
               cbdc::parsec::runtime_locking_shard::value_type>(res)) {
            auto cb_res = std::get<cbdc::parsec::runtime_locking_shard::value_type>(res);
            log->trace("Found this (callback):", cb_res.c_str());
            return_value = cb_res;
        }
        else {
            log->error("get row callback recieved error");
        }
    });
    std::this_thread::sleep_for(std::chrono::seconds(10));
    log->trace("Found this:", return_value.c_str());
    return 0;
}
