// Copyright (c) 2021 MIT Digital Currency Initiative,
//                    Federal Reserve Bank of Boston
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include "commitment.hpp"

#include "crypto/sha256.h"
#include "keys.hpp"

#include <array>
#include <cstring>
#include <secp256k1_generator.h>
#include <sstream>
#include <vector>

namespace cbdc {
    auto
    commit(const secp256k1_context* ctx, uint64_t value, const hash_t& blind)
        -> std::optional<secp256k1_pedersen_commitment> {
        secp256k1_pedersen_commitment commit{};
        auto res = secp256k1_pedersen_commit(ctx,
                                             &commit,
                                             blind.data(),
                                             value,
                                             secp256k1_generator_h);
        if(res != 1) {
            return std::nullopt;
        }

        return commit;
    }

    auto serialize_commitment(const secp256k1_context* ctx,
                              secp256k1_pedersen_commitment comm)
        -> commitment_t {
        commitment_t c{};
        secp256k1_pedersen_commitment_serialize(ctx, c.data(), &comm);
        return c;
    }

    auto make_commitment(const secp256k1_context* ctx,
                         uint64_t value,
                         const hash_t& blind) -> std::optional<commitment_t> {
        auto comm = commit(ctx, value, blind);
        if(!comm.has_value()) {
            return std::nullopt;
        }

        return serialize_commitment(ctx, comm.value());
    }

    auto deserialize_commitment(const secp256k1_context* ctx,
                                commitment_t comm)
        -> std::optional<secp256k1_pedersen_commitment> {
        secp256k1_pedersen_commitment commitment{};
        if(secp256k1_pedersen_commitment_parse(ctx, &commitment, comm.data())
           != 1) {
            return std::nullopt;
        }

        return commitment;
    }

    auto sum_commitments(const secp256k1_context* ctx,
                         std::vector<commitment_t> commitments)
        -> std::optional<commitment_t> {
        if(commitments.size() == 0) {
            return std::nullopt;
        } else if(commitments.size() == 1) {
            return {commitments[0]};
        }

        std::vector<secp256k1_pubkey> as_keys{};
        for(auto& c : commitments) {
            auto maybe_pc = deserialize_commitment(ctx, c);
            if(!maybe_pc.has_value()) {
                return std::nullopt;
            }

            auto pc = maybe_pc.value();
            secp256k1_pubkey k{};
            secp256k1_pedersen_commitment_as_key(&pc, &k);
            as_keys.emplace_back(k);
        }

        secp256k1_pubkey k{};
        auto res =
            secp256k1_ec_pubkey_combine(ctx, &k,
                reinterpret_cast<const secp256k1_pubkey* const*>(as_keys.data()),
                as_keys.size());
        if(res != 1) {
            return std::nullopt;
        }

        secp256k1_pedersen_commitment summary{};
        secp256k1_pubkey_as_pedersen_commitment(ctx, &k, &summary);

        return serialize_commitment(ctx, summary);
    }
}
