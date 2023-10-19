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
#include <thread>

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

        // std::string michael = "michael";
        // auto k = cbdc::buffer();
        // k.append(michael.c_str(), michael.size());
        // auto res = cbdc::buffer();
        // res.append("this is garbage", 16);
        // get_value_at(k);
        // m_log->trace("what we found", res.c_str());
        Py_Initialize();

        // to pass in args, use Py_BuildValue() to build string

        /*
            Next Step:
            Define a way for functions to have signatures
                Specifically, what are the output(s) of the function
                and their types.
        */

        // m_log->trace(m_function.c_str());
        PyObject* main = PyImport_AddModule("__main__");
        PyObject* globalDictionary = PyModule_GetDict(main);
        PyObject* localDictionary = PyDict_New();
        //
        parse_header();
        auto params = parse_params();
        // Need to determine a scheme to create a list of argument names with
        // values store each function as {n_args, arg_names, function_script}
        // args.push_back("website1");
        // args.push_back("website2");

        // int argc = std::stoi(params[0]);

        // m_log->trace("param count: ", params.size());
        if(m_input_args.size() < params.size()) {
            m_log->error("Too few arguments passed to function");
            return true;
        }
        for(unsigned long i = 0; i < params.size(); i++) {
            PyObject* value = PyUnicode_FromString(params[i].c_str());
            PyDict_SetItemString(localDictionary,
                                 m_input_args[i].c_str(),
                                 value);
        }
        // introduce a callback into C++ that can interact with shards

        // passing arguments
        // PyDict_SetItemString(localDictionary,
        //                      "account",
        //                      PyUnicode_FromString(""));
        // PyDict_SetItemString(localDictionary,
        //                      "new_balance",
        //                      PyLong_FromLong(0));
        // m_log->trace(m_function.c_str());
        auto r = PyRun_String(m_function.c_str(),
                              Py_file_input,
                              globalDictionary,
                              localDictionary);

        // update_state();
        // long result;
        // auto res = PyDict_GetItemString(localDictionary, "website1");
        // if(!PyArg_ParseTuple(res, "l", result)) {
        //     m_log->error("tuple could not be parsed");
        // }
        // else {
        //     m_log->trace("Tuple parsed", result);
        // }
        auto value
            = PyLong_AsLong(PyDict_GetItemString(localDictionary, "website1"));
        m_log->trace("Website1 = ", value);
        // update_state(localDictionary);
        //  schedule_contract();
        if(!r) {
            m_log->error("R = ", r);
            m_log->error("PyRun had error");
        }
        if(Py_FinalizeEx() < 0) {
            m_log->fatal("Py not finalized correctly");
        }

        // m_result_callback(error_code::exec_error); // REALLY SHOULD CALL THE
        //  CALLBACK SOMEWHERE!

        // m_result_callback(error_code::exec_error);
        // m_log->info("calling run");

        // get_value_at(k);
        // res = cbdc::buffer();
        // res.append("this is garbage", 16);
        // if(m_return_values.size() > 0) {
        //     res = m_return_values[0];
        // }
        // m_log->trace("what we found", res.c_str());

        // call m_result_callback with "state update type" or error code
        auto results = runtime_locking_shard::state_update_type();
        auto key_buf = cbdc::buffer();
        auto value_buf = cbdc::buffer();
        key_buf.append("some key", 8);
        value_buf.append("some value", 10);
        results.emplace(std::move(key_buf),
                        std::move(value_buf));
        m_result_callback(std::move(results));

        return true;
    }

    // fills m_input_args, m_return_args, m_return_types
    void py_runner::parse_header() {
        /* Assumes that header is return types | return args | input args |
         * func */
        auto charPtr = std::string((char*)m_function.data());

        m_input_args.clear();
        m_return_args.clear();
        m_return_types.clear();
        // need to delete the start of m_function

        // parse the return types of the header
        auto arg_delim = charPtr.find('|');
        auto arg_string = charPtr.substr(0, arg_delim);
        size_t pos = 0;
        m_return_types = arg_string;
        // while((pos = arg_string.find(",", 0)) != std::string::npos) {
        //     m_return_types.push_back(arg_string.substr(0, pos));
        //     arg_string.erase(0, pos + 1);
        // }
        charPtr.erase(0, arg_delim + 1);

        arg_delim = charPtr.find('|');
        arg_string = charPtr.substr(0, arg_delim);
        pos = 0;
        while((pos = arg_string.find(",", 0)) != std::string::npos) {
            m_return_args.push_back(arg_string.substr(0, pos));
            arg_string.erase(0, pos + 1);
        }
        charPtr.erase(0, arg_delim + 1);

        arg_delim = charPtr.find('|');
        arg_string = charPtr.substr(0, arg_delim);
        pos = 0;
        while((pos = arg_string.find(",", 0)) != std::string::npos) {
            m_input_args.push_back(arg_string.substr(0, pos));
            arg_string.erase(0, pos + 1);
        }
        charPtr.erase(0, arg_delim + 1);

        m_function = cbdc::buffer();
        m_function.append(charPtr.c_str(), charPtr.size());
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
        char* key;
        // for(std::string v : m_return_args) {
        //     // determine type of arg then parse
        //     auto ret_val = PyDict_GetItemString(localDictionary, v);
        //     Py_TYPE(ret_val);
        // }
        if(PyUnicode_Check(PyDict_GetItemString(localDictionary, "account"))) {
            m_log->trace("unicode check passed");
            key = PyBytes_AS_STRING(PyUnicode_AsEncodedString(
                PyDict_GetItemString(localDictionary, "account"),
                "UTF-8",
                "strict"));
        } else {
            key = PyBytes_AsString(
                PyDict_GetItemString(localDictionary, "account"));
        }
        auto value = PyLong_AsLong(
            PyDict_GetItemString(localDictionary, "new_balance"));
        auto key_buf = cbdc::buffer();
        key_buf.append(key, strlen(key) + 1);
        auto value_buf = cbdc::buffer();
        value_buf.append(&value, 4);
        updates.emplace(key_buf,
                        std::move(value_buf)); // ought to use std::move
        m_log->trace("key:", key);
        m_log->trace("value", value);
        auto success = m_try_lock_callback(key_buf, // ought to use std::move
                                           broker::lock_type::write,
                                           [&](auto res) {
                                               handle_try_lock(res);
                                           });
        if(!success) {
            m_log->error("Failed to issue try lock command");
            m_result_callback(error_code::internal_error);
        }

        m_result_callback(std::move(updates));
    }

    /*
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
        // auto py_runner::get_stack_string(int index) -> std::optional<buffer>
       {
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
    */
    void py_runner::handle_try_lock(
        const broker::interface::try_lock_return_type& res) {
        auto maybe_error = std::visit(
            overloaded{[&]([[maybe_unused]] const broker::value_type& v)
                           -> std::optional<error_code> {
                           m_log->trace("broker return", v.c_str());
                           // do something with what the shard returns why
                           // don't you you shmuck
                           //    m_return_values.push_back(v);
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
        return;
        // schedule_contract();
    }

    void py_runner::get_value_at(runtime_locking_shard::key_type key) {
        // auto success =
        m_try_lock_callback(std::move(key), // try locking the key
                            broker::lock_type::read,
                            [&](auto res) {
                                handle_try_lock(res);
                            });

        /*
        Need to follow how the lua runner does it:
        - pushes value to lua state
        - calls again ("resumes")
        - Need to do this in an orderly way such that the value is finalized
            upon accessing it
        */

        /*
         How about try:
         Ask for value k:
         if k not contianed in set of requested values, add it to the set
         if k contained: wait (?)
        */

        // if(!success) {
        //     m_log->error("Failed to issue try lock command");
        //     m_result_callback(error_code::internal_error);
        //     // return cbdc::buffer(); // this should be an error
        // }
        // else {
        //     if(!m_return_values.size()) {
        //         return cbdc::buffer();
        //     }
        //     auto ret = m_return_values[0];
        //     m_return_values.pop_back();
        //     return ret; // this should be what comes from the shard?
        // }
    }

    // auto py_runner::check_sig(lua_State* L) -> int {
    //     return 0;
    // }
}
