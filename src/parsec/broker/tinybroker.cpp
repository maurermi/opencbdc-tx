// Copyright (c) 2021 MIT Digital Currency Initiative,
//                    Federal Reserve Bank of Boston
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include "tinybroker.hpp"

#include "ticket_machine/interface.hpp"
#include "util/common/variant_overloaded.hpp"

#include <cassert>

namespace cbdc::parsec::broker {
    tinybroker::tinybroker(
        runtime_locking_shard::broker_id_type broker_id,
        std::shared_ptr<logging::log> logger)
        : m_broker_id(broker_id),
          m_log(std::move(logger)) {}

    auto tinybroker::begin(begin_callback_type result_callback) -> bool {
        // TODO: Set a variable for the ticket number range
        m_highest_ticket++;
        result_callback(m_highest_ticket);
        return true;
    }

    auto tinybroker::highest_ticket() -> ticket_number_type {
        return m_highest_ticket;
    }

    void tinybroker::handle_lock(
        ticket_number_type ticket_number,
        key_type key,
        uint64_t shard_idx,
        const try_lock_callback_type& result_callback,
        const parsec::runtime_locking_shard::interface::try_lock_return_type&
            res) {
        auto result = std::visit(
            overloaded{
                [&](parsec::runtime_locking_shard::value_type v)
                    -> try_lock_return_type {
                    std::unique_lock l(m_mut);
                    auto it = m_tickets.find(ticket_number);
                    if(it == m_tickets.end()) {
                        return error_code::unknown_ticket;
                    }

                    auto t_state = it->second;
                    auto& s_state = t_state->m_shard_states[shard_idx];
                    auto k_it = s_state.m_key_states.find(key);
                    if(k_it == s_state.m_key_states.end()) {
                        m_log->error("Shard state not found for key");
                        return error_code::invalid_shard_state;
                    }

                    if(k_it->second.m_key_state != key_state::locking) {
                        m_log->error("Shard state not locking");
                        return error_code::invalid_shard_state;
                    }

                    k_it->second.m_key_state = key_state::locked;
                    k_it->second.m_value = v;

                    m_log->trace(this, "Broker locked key for", ticket_number);

                    return v;
                },
                [&, key](parsec::runtime_locking_shard::shard_error e)
                    -> try_lock_return_type {
                    if(e.m_wounded_details.has_value()) {
                        m_log->trace(this,
                                     e.m_wounded_details->m_wounding_ticket,
                                     "wounded ticket",
                                     ticket_number);
                    }
                    m_log->trace(this,
                                 "Shard error",
                                 static_cast<int>(e.m_error_code),
                                 "locking key",
                                 key.to_hex(),
                                 "for",
                                 ticket_number);
                    return e;
                }},
            res);
        result_callback(result);
    }

    auto tinybroker::try_lock(ticket_number_type ticket_number,
                              key_type key,
                              lock_type locktype,
                              try_lock_callback_type result_callback) -> bool {
        auto maybe_error = [&]() -> std::optional<error_code> {
            std::unique_lock l(m_mut);
            auto it = m_tickets.find(ticket_number);
            if(it == m_tickets.end()) {
                return error_code::unknown_ticket;
            }

            auto t_state = it->second;
            switch(t_state->m_state) {
                case ticket_state::begun:
                    break;
                case ticket_state::prepared:
                    return error_code::prepared;
                case ticket_state::committed:
                    return error_code::committed;
                case ticket_state::aborted:
                    t_state->m_state = ticket_state::begun;
                    t_state->m_shard_states.clear();
                    m_log->trace(this, "broker restarting", ticket_number);
                    break;
            }

            return std::nullopt;
        }();

        if(maybe_error.has_value()) {
            result_callback(maybe_error.value());
        }

        auto res = m_state.find(key);
        if(res == m_state.end()) {
            auto to_place = state_element_type{};
            if(locktype == lock_type::write) {
                to_place.m_lock.m_writer = ticket_number;
            } else {
                to_place.m_lock.m_readers.insert(ticket_number);
            }
            to_place.m_value = value_type{};
            m_state[key] = to_place;
            result_callback(to_place.m_value);
        } else {
            auto to_place = res->second;
            result_callback(to_place.m_value);
            if(locktype == lock_type::write) {
                to_place.m_lock.m_writer = ticket_number;
            } else {
                to_place.m_lock.m_readers.insert(ticket_number);
            }
        }

        return true;
    }

    void tinybroker::handle_prepare(
        const commit_callback_type& commit_cb,
        ticket_number_type ticket_number,
        uint64_t shard_idx,
        parsec::runtime_locking_shard::interface::prepare_return_type res) {
        auto maybe_error = [&]() -> std::optional<commit_return_type> {
            std::unique_lock ll(m_mut);
            auto itt = m_tickets.find(ticket_number);
            if(itt == m_tickets.end()) {
                return error_code::unknown_ticket;
            }

            auto ts = itt->second;
            switch(ts->m_state) {
                case ticket_state::begun:
                    break;
                case ticket_state::prepared:
                    return error_code::prepared;
                case ticket_state::committed:
                    return error_code::committed;
                case ticket_state::aborted:
                    return error_code::aborted;
            }

            return do_handle_prepare(commit_cb,
                                     ticket_number,
                                     ts,
                                     shard_idx,
                                     res);
        }();

        m_log->trace(this, "Broker handled prepare for", ticket_number);

        if(maybe_error.has_value()) {
            m_log->trace(this,
                         "Broker calling prepare callback with error for",
                         ticket_number);
            commit_cb(maybe_error.value());
        }
    }

    auto tinybroker::do_handle_prepare(
        const commit_callback_type& commit_cb,
        ticket_number_type ticket_number,
        const std::shared_ptr<state>& ts,
        uint64_t shard_idx,
        const parsec::runtime_locking_shard::interface::prepare_return_type&
            res) -> std::optional<commit_return_type> {
        auto& ss = ts->m_shard_states[shard_idx].m_state;
        if(ss != shard_state_type::preparing) {
            m_log->trace(this,
                         "Shard",
                         shard_idx,
                         "not in preparing state for",
                         ticket_number);
            return std::nullopt;
        }

        if(res.has_value()) {
            if(res.value().m_error_code
               != runtime_locking_shard::error_code::wounded) {
                m_log->error("Shard error with prepare for", ticket_number);
            } else {
                m_log->trace("Shard",
                             shard_idx,
                             "wounded ticket",
                             ticket_number);
                for(auto& [sidx, s] : ts->m_shard_states) {
                    if(s.m_state == shard_state_type::wounded) {
                        return std::nullopt;
                    }
                }
                ss = shard_state_type::wounded;
            }
            return res.value();
        }

        m_log->trace(this,
                     "Broker setting shard",
                     shard_idx,
                     "to prepared for",
                     ticket_number);
        ss = shard_state_type::prepared;

        for(auto& shard : ts->m_shard_states) {
            if(shard.second.m_state != shard_state_type::prepared) {
                return std::nullopt;
            }
        }

        ts->m_state = ticket_state::prepared;

        auto maybe_error = do_commit(commit_cb, ticket_number, ts);
        if(maybe_error.has_value()) {
            return maybe_error.value();
        }
        return std::nullopt;
    }

    auto tinybroker::do_commit(const commit_callback_type& commit_cb,
                               ticket_number_type ticket_number,
                               const std::shared_ptr<state>& ts)
        -> std::optional<error_code> {
        for(auto& shard : ts->m_shard_states) {
            if(ts->m_state == ticket_state::aborted) {
                m_log->trace("Broker aborted during commit for",
                             ticket_number);
                break;
            }
            if(shard.second.m_state == shard_state_type::committed) {
                continue;
            }
            shard.second.m_state = shard_state_type::committing;
            auto sidx = shard.first;
            if(!m_shards[sidx]->commit(
                   ticket_number,
                   [=, this](const parsec::runtime_locking_shard::interface::
                                 commit_return_type& comm_res) {
                       handle_commit(commit_cb, ticket_number, sidx, comm_res);
                   })) {
                m_log->error("Failed to make commit shard request");
                return error_code::shard_unreachable;
            }
        }
        return std::nullopt;
    }

    void tinybroker::handle_commit(
        const commit_callback_type& commit_cb,
        ticket_number_type ticket_number,
        uint64_t shard_idx,
        parsec::runtime_locking_shard::interface::commit_return_type res) {
        auto callback = false;
        auto maybe_error = [&]() -> std::optional<error_code> {
            std::unique_lock lll(m_mut);
            auto ittt = m_tickets.find(ticket_number);
            if(ittt == m_tickets.end()) {
                return error_code::unknown_ticket;
            }

            auto tss = ittt->second;
            switch(tss->m_state) {
                case ticket_state::begun:
                    return error_code::not_prepared;
                case ticket_state::prepared:
                    break;
                case ticket_state::committed:
                    return error_code::committed;
                case ticket_state::aborted:
                    return error_code::aborted;
            }

            if(tss->m_shard_states[shard_idx].m_state
               != shard_state_type::committing) {
                m_log->error("Commit result when shard not committing");
                return error_code::invalid_shard_state;
            }

            if(res.has_value()) {
                m_log->error("Error committing on shard");
                return error_code::commit_error;
            }

            tss->m_shard_states[shard_idx].m_state
                = shard_state_type::committed;

            for(auto& shard : tss->m_shard_states) {
                if(shard.second.m_state != shard_state_type::committed) {
                    return std::nullopt;
                }
            }

            tss->m_state = ticket_state::committed;
            callback = true;

            m_log->trace(this, "Broker handled commit for", ticket_number);

            return std::nullopt;
        }();

        if(maybe_error.has_value()) {
            m_log->trace(this,
                         "Broker calling commit callback with error for",
                         ticket_number, maybe_error.value());
            commit_cb(maybe_error.value());
        } else if(callback) {
            m_log->trace(this,
                         "Broker calling commit callback from handle_commit "
                         "with success for",
                         ticket_number);
            commit_cb(std::nullopt);
        }
    }

    auto tinybroker::commit(ticket_number_type ticket_number,
                            state_update_type state_updates,
                            commit_callback_type result_callback) -> bool {
        m_log->trace(this,
                     "Tiny broker got commit request for",
                     ticket_number);
        auto maybe_error = [&]() -> std::optional<error_code> {
            std::unique_lock l(m_mut);
            auto it = m_tickets.find(ticket_number);
            if(it == m_tickets.end()) {
                return error_code::unknown_ticket;
            }

            auto t_state = it->second;
            switch(t_state->m_state) {
                case ticket_state::begun:
                    [[fallthrough]];
                case ticket_state::prepared:
                    break;
                case ticket_state::committed:
                    return error_code::committed;
                case ticket_state::aborted:
                    return error_code::aborted;
            }

            for(auto update : state_updates) {
                if(m_state[update.first].m_lock.m_writer != ticket_number) {
                    return error_code::lock_not_held;
                }
            }
            return std::nullopt;
        }();

        if(maybe_error.has_value()) {
            m_log->trace(
                this,
                "Broker calling commit callback with error from commit for",
                ticket_number);
            result_callback(maybe_error.value());
        } else {
            for(auto update : state_updates) {
                m_state[update.first].m_value = update.second;
            }
            result_callback(std::nullopt);
        }

        return true;
    }

    auto tinybroker::finish(ticket_number_type ticket_number,
                            finish_callback_type result_callback) -> bool {
        auto maybe_error = [&]() -> std::optional<error_code> {
            std::unique_lock l(m_mut);
            auto it = m_tickets.find(ticket_number);
            if(it == m_tickets.end()) {
                m_log->trace(this,
                             "Broker failing finish: [Unknown ticket] for ",
                             ticket_number);
                return error_code::unknown_ticket;
            }

            auto t_state = it->second;
            switch(t_state->m_state) {
                case ticket_state::begun:
                    m_log->trace(this,
                                 "Broker failing finish: [State = Begun] for ",
                                 ticket_number);
                    return error_code::begun;
                case ticket_state::prepared:
                    m_log->trace(
                        this,
                        "Broker failing finish: [State = Prepared] for ",
                        ticket_number);
                    return error_code::prepared;
                case ticket_state::committed:
                    break;
                case ticket_state::aborted:
                    // Ticket already rolled back. Just delete the ticket.
                    m_tickets.erase(it);
                    return std::nullopt;
            }

            return std::nullopt;
        }();

        if(maybe_error.has_value()) {
            result_callback(maybe_error.value());
        } else {
            result_callback(std::nullopt);
        }
        // TODO: somehow need to delete readers and writers
        return true;
    }

    auto tinybroker::rollback(ticket_number_type ticket_number,
                              rollback_callback_type result_callback) -> bool {
        m_log->trace(this, "Broker got rollback request for", ticket_number);
        auto callback = false;
        auto maybe_error = [&]() -> std::optional<error_code> {
            std::unique_lock l(m_mut);
            auto it = m_tickets.find(ticket_number);
            if(it == m_tickets.end()) {
                return error_code::unknown_ticket;
            }

            return std::nullopt;
        }();

        m_log->trace(this,
                     "Broker initiated rollback request for",
                     ticket_number);

        if(maybe_error.has_value()) {
            result_callback(maybe_error.value());
        } else if(callback) {
            result_callback(std::nullopt);
        }

        m_log->trace(this,
                     "Broker handled rollback request for",
                     ticket_number);

        return true;
    }

    void tinybroker::handle_rollback(
        const rollback_callback_type& result_callback,
        ticket_number_type ticket_number,
        uint64_t shard_idx,
        parsec::runtime_locking_shard::interface::rollback_return_type res) {
        return;
    }

    void tinybroker::handle_find_key(
        ticket_number_type ticket_number,
        key_type key,
        lock_type locktype,
        try_lock_callback_type result_callback,
        std::optional<parsec::directory::interface::key_location_return_type>
            res) {
        return;
    }

    void tinybroker::handle_finish(
        const finish_callback_type& result_callback,
        ticket_number_type ticket_number,
        uint64_t shard_idx,
        parsec::runtime_locking_shard::interface::finish_return_type res) {
        return;
    }

    auto tinybroker::recover(recover_callback_type result_callback) -> bool {
        return true;
    }

    void tinybroker::handle_get_tickets(
        const recover_callback_type& result_callback,
        uint64_t shard_idx,
        const parsec::runtime_locking_shard::interface::
            get_tickets_return_type& res) {
        return;
    }

    auto tinybroker::do_recovery(const recover_callback_type& result_callback)
        -> std::optional<error_code> {
        return std::nullopt;
    }

    void tinybroker::handle_recovery_commit(
        const recover_callback_type& result_callback,
        ticket_number_type ticket_number,
        const commit_return_type& res) {
        return;
    }

    void tinybroker::handle_recovery_finish(
        const recover_callback_type& result_callback,
        finish_return_type res) {
        return;
    }

    void tinybroker::handle_recovery_rollback(
        const recover_callback_type& result_callback,
        ticket_number_type ticket_number,
        rollback_return_type res) {
        return;
    }
}
