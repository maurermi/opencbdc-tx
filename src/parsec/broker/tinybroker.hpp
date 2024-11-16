// Copyright (c) 2021 MIT Digital Currency Initiative,
//                    Federal Reserve Bank of Boston
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef OPENCBDC_TX_SRC_PARSEC_BROKER_TINYBROKER_H_
#define OPENCBDC_TX_SRC_PARSEC_BROKER_TINYBROKER_H_

#include "interface.hpp"
#include "parsec/directory/interface.hpp"
#include "util/common/logging.hpp"

#include <memory>
#include <unordered_set>
#include <unordered_map>
#include <map>


namespace cbdc::parsec::broker {
    /// Implementation of a broker. Stores ticket states in memory.
    /// Thread-safe.
    class tinybroker : public interface {
      public:
        /// Constructor.
        /// \param broker_id unique ID of this broker instance.
        /// \param shards vector of shard instances.
        /// \param ticketer ticket machine instance.
        /// \param directory directory instance.
        /// \param logger log instance.
        tinybroker(
            runtime_locking_shard::broker_id_type broker_id,
            std::shared_ptr<logging::log> logger);

        /// Requests a new ticket number from the ticket machine.
        /// \param result_callback function to call with the begin result.
        /// \return true if the request to the ticket machine was initiated
        ///         successfully.
        auto begin(begin_callback_type result_callback) -> bool override;

        /// Determines the shard responsible for the given key and issues a try
        /// lock request for the key.
        /// \param ticket_number ticket number.
        /// \param key key to lock.
        /// \param locktype type of lock to acquire.
        /// \param result_callback function to call with try_lock request.
        /// \return true: if request to the directory was initiated
        ///         successfully.
        ///         false: only if an unexpected exception was encountered
        auto try_lock(ticket_number_type ticket_number,
                      key_type key,
                      lock_type locktype,
                      try_lock_callback_type result_callback) -> bool override;

        /// Commits the ticket on all shards involved in the ticket.
        /// \param ticket_number ticket number.
        /// \param state_updates state updates to apply if ticket commits.
        /// \param result_callback function to call with commit result.
        /// \return true if all requests to shards were initiated successfully.
        auto commit(ticket_number_type ticket_number,
                    state_update_type state_updates,
                    commit_callback_type result_callback) -> bool override;

        /// Finishes the ticket on all shards involved in the ticket.
        /// \param ticket_number ticket number.
        /// \param result_callback function to call with finish result.
        /// \return true if requests to all shards were initiated successfully.
        auto finish(ticket_number_type ticket_number,
                    finish_callback_type result_callback) -> bool override;

        /// Rolls back the ticket on all shards involved in the ticket.
        /// \param ticket_number ticket number.
        /// \param result_callback function to call with rollback result.
        /// \return true if requests to all shard were initiated successfully.
        auto rollback(ticket_number_type ticket_number,
                      rollback_callback_type result_callback) -> bool override;

        /// Requests tickets managed by this broker ID from all shards and
        /// completes partially committed tickets, and rolls back all other
        /// tickets. Finishes all tickets.
        /// \param result_callback function to call with recovery result.
        /// \return true if requests to all shards were initiated successfully.
        auto recover(recover_callback_type result_callback) -> bool override;

        /// Get the highest ticket number that was used. This is not to be
        /// used for calculating a next ticket number, but is used to calculate
        /// the pretend height of the chain in the evm runner, which is derived
        /// from ticket numbers
        /// \return highest ticket number that was used
        auto highest_ticket() -> ticket_number_type override;

      private:
        enum class ticket_state : uint8_t {
            begun,
            prepared,
            committed,
            aborted
        };

        runtime_locking_shard::broker_id_type m_broker_id;
        std::vector<std::shared_ptr<runtime_locking_shard::interface>>
            m_shards;
        std::shared_ptr<ticket_machine::interface> m_ticketer;
        std::shared_ptr<directory::interface> m_directory;
        std::shared_ptr<logging::log> m_log;

        mutable std::recursive_mutex m_mut;
        ticket_number_type m_highest_ticket{0};

        enum class shard_state_type : uint8_t {
            begun,
            preparing,
            prepared,
            wounded,
            committing,
            committed,
            rolling_back,
            rolled_back,
            finishing,
            finished
        };

        enum class key_state : uint8_t {
            locking,
            locked
        };

        struct key_state_type {
            key_state m_key_state{};
            lock_type m_locktype{};
            std::optional<value_type> m_value;
        };

        struct shard_state {
            std::unordered_map<key_type,
                               key_state_type,
                               hashing::const_sip_hash<key_type>>
                m_key_states;
            shard_state_type m_state{};
        };

        using shard_states = std::unordered_map<size_t, shard_state>;

        struct state {
            ticket_state m_state{};
            shard_states m_shard_states;
        };

        std::unordered_map<ticket_number_type, std::shared_ptr<state>>
            m_tickets;

        std::unordered_map<
            uint64_t,
            std::unordered_map<ticket_number_type,
                               runtime_locking_shard::ticket_state>>
            m_recovery_tickets;

        void handle_prepare(
            const commit_callback_type& commit_cb,
            ticket_number_type ticket_number,
            uint64_t shard_idx,
            parsec::runtime_locking_shard::interface::prepare_return_type res);

        void handle_commit(
            const commit_callback_type& commit_cb,
            ticket_number_type ticket_number,
            uint64_t shard_idx,
            parsec::runtime_locking_shard::interface::commit_return_type res);

        void handle_lock(ticket_number_type ticket_number,
                         key_type key,
                         uint64_t shard_idx,
                         const try_lock_callback_type& result_callback,
                         const parsec::runtime_locking_shard::interface::
                             try_lock_return_type& res);

        void handle_ticket_number(
            begin_callback_type result_callback,
            std::optional<parsec::ticket_machine::interface::
                              get_ticket_number_return_type> res);

        void handle_rollback(
            const rollback_callback_type& result_callback,
            ticket_number_type ticket_number,
            uint64_t shard_idx,
            parsec::runtime_locking_shard::interface::rollback_return_type
                res);

        void handle_find_key(
            ticket_number_type ticket_number,
            key_type key,
            lock_type locktype,
            try_lock_callback_type result_callback,
            std::optional<
                parsec::directory::interface::key_location_return_type> res);

        void handle_finish(
            const finish_callback_type& result_callback,
            ticket_number_type ticket_number,
            uint64_t shard_idx,
            parsec::runtime_locking_shard::interface::finish_return_type res);

        void handle_get_tickets(const recover_callback_type& result_callback,
                                uint64_t shard_idx,
                                const parsec::runtime_locking_shard::
                                    interface::get_tickets_return_type& res);

        void
        handle_recovery_commit(const recover_callback_type& result_callback,
                               ticket_number_type ticket_number,
                               const commit_return_type& res);

        void
        handle_recovery_finish(const recover_callback_type& result_callback,
                               finish_return_type res);

        void
        handle_recovery_rollback(const recover_callback_type& result_callback,
                                 ticket_number_type ticket_number,
                                 rollback_return_type res);

        auto do_commit(const commit_callback_type& commit_cb,
                       ticket_number_type ticket_number,
                       const std::shared_ptr<state>& ts)
            -> std::optional<error_code>;

        auto do_handle_prepare(const commit_callback_type& commit_cb,
                               ticket_number_type ticket_number,
                               const std::shared_ptr<state>& ts,
                               uint64_t shard_idx,
                               const parsec::runtime_locking_shard::interface::
                                   prepare_return_type& res)
            -> std::optional<commit_return_type>;

        auto do_prepare(const commit_callback_type& result_callback,
                        ticket_number_type ticket_number,
                        const std::shared_ptr<state>& t_state,
                        const state_update_type& state_updates)
            -> std::optional<error_code>;

        auto do_recovery(const recover_callback_type& result_callback)
            -> std::optional<error_code>;

        struct lock_queue_element_type {
            lock_type m_type;
            try_lock_callback_type m_callback;
        };

        struct rw_lock_type {
            std::optional<ticket_number_type> m_writer;
            std::unordered_set<ticket_number_type> m_readers;
            std::map<ticket_number_type, lock_queue_element_type> m_queue;
        };

        struct state_element_type {
            value_type m_value;
            rw_lock_type m_lock;
        };

        using key_set_type
            = std::unordered_set<key_type, hashing::const_sip_hash<key_type>>;

        struct ticket_state_type {
            ticket_state m_state{ticket_state::begun};
            std::unordered_map<key_type,
                               lock_type,
                               hashing::const_sip_hash<key_type>>
                m_locks_held;
            key_set_type m_queued_locks;
            state_update_type m_state_update;
            runtime_locking_shard::broker_id_type m_broker_id{};
            std::optional<runtime_locking_shard::wounded_details> m_wounded_details{};
        };

        struct pending_callback_element_type {
            try_lock_callback_type m_callback;
            try_lock_return_type m_returning;
            ticket_number_type m_ticket_number;
        };

        using pending_callbacks_list_type
            = std::vector<pending_callback_element_type>;


        std::unordered_map<key_type,
                           state_element_type,
                           hashing::const_sip_hash<key_type>>
            m_state;

        auto
        wound_tickets(key_type key,
                      const std::vector<ticket_number_type>& blocking_tickets,
                      ticket_number_type blocked_ticket)
            -> pending_callbacks_list_type;

        static auto get_waiting_on(ticket_number_type ticket_number,
                                   lock_type locktype,
                                   rw_lock_type& lock)
            -> std::vector<ticket_number_type>;

        auto release_locks(ticket_number_type ticket_number,
                           ticket_state_type& ticket)
            -> std::pair<pending_callbacks_list_type, key_set_type>;

        auto acquire_locks(const key_set_type& keys)
            -> pending_callbacks_list_type;

        auto acquire_lock(const key_type& key,
                          pending_callbacks_list_type& callbacks) -> bool;
    };
}

#endif
