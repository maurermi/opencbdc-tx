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

        // to pass in args, use Py_BuildValue() to build string

        // m_log->trace(m_function.c_str());
        PyObject* main = PyImport_AddModule("__main__");
        PyObject* globalDictionary = PyModule_GetDict(main);
        PyObject* localDictionary = PyDict_New();
        std::vector<std::string> args;
        // args.push_back("website1");
        // args.push_back("website2");
        // auto params = parse_params();
        // m_log->trace("param count: ", params.size());
        // for(unsigned long i = 0; i < params.size(); i++) {
        //     PyObject* value = PyUnicode_FromString(params[i].c_str());
        //     PyDict_SetItemString(localDictionary, args[i].c_str(), value);
        // }
        // passing arguments
        PyDict_SetItemString(localDictionary,
                             "account",
                             PyUnicode_FromString(""));
        PyDict_SetItemString(localDictionary,
                             "new_balance",
                             PyLong_FromLong(0));
        m_log->trace(m_function.c_str());
        auto r = PyRun_String(m_function.c_str(),
                              Py_file_input,
                              globalDictionary,
                              localDictionary);

        // update_state();
        update_state(localDictionary);
        // schedule_contract();
        if(r) {
            m_log->error("R = ", r);
            m_log->error("PyRun had error");
        }
        if(Py_FinalizeEx() < 0) {
            m_log->fatal("Py not finalized correctly");
        }
        return true;
    }

    auto py_runner::parse_params() -> std::vector<std::string> {
        std::vector<std::string> params;
        char* charPtr = (char*)m_param.data();
        // thanks chatgpt
        while(*charPtr != '\0') {
            params.emplace_back(
                charPtr); // Create a string from the current position
            charPtr += params.back().length()
                     + 1; // Move the pointer to the next string
        }

        return params;
    }

    void py_runner::update_state(PyObject* localDictionary) {
        auto updates = runtime_locking_shard::state_update_type();
        // if(PyUnicode_Check(PyDict_GetItemString(localDictionary,
        // "account"))) {
        //     m_log->trace("OK");
        // }
        // else {
        //     m_log->error("NOT OK");
        // }
        char *key;
        if(PyUnicode_Check(PyDict_GetItemString(localDictionary, "account"))) {
            m_log->trace("unicode check passed");
            key = PyBytes_AS_STRING(PyUnicode_AsEncodedString(
                PyDict_GetItemString(localDictionary, "account"), "UTF-8", "strict"));
        } else {
            key = PyBytes_AsString(
                PyDict_GetItemString(localDictionary, "account"));
        }
        auto value = PyLong_AsLong(
            PyDict_GetItemString(localDictionary, "new_balance"));
        auto key_buf = cbdc::buffer();
        key_buf.append("3B2F51dad57e4160fd51DdB9A502c320B3f6363f", 41);
        auto value_buf = cbdc::buffer();
        value_buf.append("100", 4);
        updates.emplace(key_buf,
                        std::move(value_buf)); // ought to use std::move
        m_log->trace("key:", key);
        m_log->trace("value", value);
        auto success = m_try_lock_callback(key_buf, // ought to use std::move
                                           broker::lock_type::write,
                                           [&](auto res) {
                                               handle_try_lock(std::move(res));
                                           });
        if(!success) {
            m_log->error("Failed to issue try lock command");
            m_result_callback(error_code::internal_error);
        }

        m_result_callback(std::move(updates));
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

    //     //contract_epilogue(0);
    // }

    void py_runner::handle_try_lock(
        const broker::interface::try_lock_return_type& res) {
        auto maybe_error = std::visit(
            overloaded{[&]([[maybe_unused]] const broker::value_type& v)
                           -> std::optional<error_code> {
                           return std::nullopt;
                       },
                       [&](const broker::interface::error_code& /* e */)
                           -> std::optional<error_code> {
                           m_log->error("Broker error acquiring lock");
                           return error_code::lock_error;
                       },
                       [&](const runtime_locking_shard::shard_error& e)
                           -> std::optional<error_code> {
                           if(e.m_error_code
                              == runtime_locking_shard::error_code::wounded) {
                               return error_code::wounded;
                           }
                           m_log->error("Shard error acquiring lock");
                           return error_code::lock_error;
                       }},
            res);
        if(maybe_error.has_value()) {
            m_result_callback(maybe_error.value());
            return;
        }
        // schedule_contract();
    }

    // auto py_runner::check_sig(lua_State* L) -> int {
    //     return 0;
    // }
}
