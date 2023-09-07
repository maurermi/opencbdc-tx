// Copyright (c) 2021 MIT Digital Currency Initiative,
//                    Federal Reserve Bank of Boston
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include "impl.hpp"

#include "crypto/sha256.h"
#include "util/common/keys.hpp"
#include "util/common/variant_overloaded.hpp"

#include <cassert>
#include <secp256k1.h>
#include <secp256k1_schnorrsig.h>

namespace cbdc::parsec::agent::runner {
    static const auto secp_context
        = std::unique_ptr<secp256k1_context,
                          decltype(&secp256k1_context_destroy)>(
            secp256k1_context_create(SECP256K1_CONTEXT_VERIFY),
            &secp256k1_context_destroy);

    py_runner::py_runner(std::shared_ptr<logging::log> logger,
                         const cbdc::parsec::config& cfg,
                         runtime_locking_shard::value_type function,
                         parameter_type param,
                         bool is_readonly_run,
                         run_callback_type result_callback,
                         try_lock_callback_type try_lock_callback,
                         std::shared_ptr<secp256k1_context> secp,
                         std::shared_ptr<thread_pool> t_pool,
                         ticket_number_type ticket_number)
        : interface(std::move(logger),
                    cfg,
                    std::move(function),
                    std::move(param),
                    is_readonly_run,
                    std::move(result_callback),
                    std::move(try_lock_callback),
                    std::move(secp),
                    std::move(t_pool),
                    ticket_number) {}

    auto py_runner::run() -> bool {
        m_log->info("calling run");
        Py_Initialize();

        m_log->trace(m_function.c_str());
        auto r = PyRun_SimpleString(m_function.c_str());
        if(r) {
            m_log->error("PyRun had error");
        }
        if(Py_FinalizeEx() < 0) {
            m_log->fatal("Py not finalized correctly");
        }

        //schedule_contract();

        return true;
    }

    // void py_runner::contract_epilogue(int n_results) {
    //     if(n_results != 1) {
    //         m_log->error("Contract returned more than one result");
    //         m_result_callback(error_code::result_count);
    //         return;
    //     }

    //     auto results = runtime_locking_shard::state_update_type();

    //     m_log->trace(this, "running calling result callback");
    //     m_result_callback(std::move(results));
    //     m_log->trace(this, "py_runner finished contract epilogue");
    // }

    // // use to pass messages from python -> c env
    // auto py_runner::get_stack_string(int index) -> std::optional<buffer> {
    //     size_t sz{};
    //     auto buf = buffer();
    //     m_state += index;
    //     buf.append(&m_state, sz);
    //     return buf;
    // }

    // void py_runner::schedule_contract() {
    //     //int n_results{};
    //     contract_epilogue(0);
    // }

    // void py_runner::handle_try_lock(
    //     const broker::interface::try_lock_return_type& res) {
    //     auto maybe_error = std::visit(
    //         overloaded{[&]([[maybe_unused]] const broker::value_type& v)
    //                        -> std::optional<error_code> {
    //                        return std::nullopt;
    //                    },
    //                    [&](const broker::interface::error_code& /* e */)
    //                        -> std::optional<error_code> {
    //                        m_log->error("Broker error acquiring lock");
    //                        return error_code::lock_error;
    //                    },
    //                    [&](const runtime_locking_shard::shard_error& e)
    //                        -> std::optional<error_code> {
    //                        if(e.m_error_code
    //                           == runtime_locking_shard::error_code::wounded) {
    //                            return error_code::wounded;
    //                        }
    //                        m_log->error("Shard error acquiring lock");
    //                        return error_code::lock_error;
    //                    }},
    //         res);
    //     if(maybe_error.has_value()) {
    //         m_result_callback(maybe_error.value());
    //         return;
    //     }
    //     schedule_contract();
    // }

    // auto py_runner::check_sig(lua_State* L) -> int {
    //     return 0;
    // }
}
