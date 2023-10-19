// Copyright (c) 2021 MIT Digital Currency Initiative,
//                    Federal Reserve Bank of Boston
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef OPENCBDC_TX_SRC_PARSEC_AGENT_PY_RUNNER_H_
#define OPENCBDC_TX_SRC_PARSEC_AGENT_PY_RUNNER_H_

#include "parsec/agent/runners/interface.hpp"
#include "parsec/util.hpp"

#include <Python.h>
#include <future>
#include <memory>

namespace cbdc::parsec::agent::runner {
    /// Py function executor. Provides an environment for contracts to execute
    class py_runner : public interface {
      public:
        /// \copydoc interface::interface()
        py_runner(std::shared_ptr<logging::log> logger,
                  const cbdc::parsec::config& cfg,
                  runtime_locking_shard::value_type function,
                  parameter_type param,
                  bool is_readonly_run,
                  run_callback_type result_callback,
                  try_lock_callback_type try_lock_callback,
                  std::shared_ptr<secp256k1_context> secp,
                  std::shared_ptr<thread_pool> t_pool,
                  ticket_number_type ticket_number);

        /// Begins function execution. Retrieves the function bytecode using a
        /// read lock and executes it with the given parameter.
        /// \return true unless a internal system error has occurred
        [[nodiscard]] auto run() -> bool override;

        /// Lock type to acquire when requesting the function code.
        static constexpr auto initial_lock_type = broker::lock_type::read;

      private:
        int m_state; // should reflect state (may not need)
        std::vector<std::string> m_input_args;
        std::vector<std::string> m_return_args;
        std::vector<cbdc::buffer> m_return_values;
        // std::vector<std::string> m_return_types;
        std::string m_return_types;

        // void update_state();
        void update_state(PyObject* localDictionary);
        void get_value_at(runtime_locking_shard::key_type key);
        // auto get_value_at_helper() -> runtime_locking_shard::value_type;

        // only creating both because ostensibly m_param and m_function
        // can be different data types
        auto parse_params() -> std::vector<std::string>;
        void parse_header();

        // void contract_epilogue(int n_results);

        // auto get_stack_string(int index) -> std::optional<buffer>;

        // void schedule_contract();

        void
        handle_try_lock(const broker::interface::try_lock_return_type& res);

        bool m_halt = true;
        std::promise<cbdc::buffer> m_val_promise;
        std::future<cbdc::buffer> m_val_fut;
    };
}

#endif
