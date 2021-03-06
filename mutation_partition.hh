/*
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <iosfwd>
#include <map>
#include <boost/intrusive/set.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/range/adaptor/indexed.hpp>
#include <boost/range/adaptor/filtered.hpp>

#include <seastar/core/bitset-iter.hh>

#include "schema.hh"
#include "tombstone.hh"
#include "keys.hh"
#include "position_in_partition.hh"
#include "atomic_cell_or_collection.hh"
#include "query-result.hh"
#include "mutation_partition_view.hh"
#include "mutation_partition_visitor.hh"
#include "utils/managed_vector.hh"
#include "hashing_partition_visitor.hh"
#include "range_tombstone_list.hh"
#include "clustering_key_filter.hh"
#include "intrusive_set_external_comparator.hh"
#include "utils/with_relational_operators.hh"

//
// Container for cells of a row. Cells are identified by column_id.
//
// All cells must belong to a single column_kind. The kind is not stored
// for space-efficiency reasons. Whenever a method accepts a column_kind,
// the caller must always supply the same column_kind.
//
// Can be used as a range of row::cell_entry.
//
class row {
    class cell_entry {
        boost::intrusive::set_member_hook<> _link;
        column_id _id;
        atomic_cell_or_collection _cell;
        friend class row;
    public:
        cell_entry(column_id id, atomic_cell_or_collection cell)
            : _id(id)
            , _cell(std::move(cell))
        { }
        cell_entry(column_id id)
            : _id(id)
        { }
        cell_entry(cell_entry&&) noexcept;
        cell_entry(const cell_entry&);

        column_id id() const { return _id; }
        const atomic_cell_or_collection& cell() const { return _cell; }
        atomic_cell_or_collection& cell() { return _cell; }

        struct compare {
            bool operator()(const cell_entry& e1, const cell_entry& e2) const {
                return e1._id < e2._id;
            }
            bool operator()(column_id id1, const cell_entry& e2) const {
                return id1 < e2._id;
            }
            bool operator()(const cell_entry& e1, column_id id2) const {
                return e1._id < id2;
            }
        };
    };

    using size_type = std::make_unsigned_t<column_id>;

    enum class storage_type {
        vector,
        set,
    };
    storage_type _type = storage_type::vector;
    size_type _size = 0;

    using map_type = boost::intrusive::set<cell_entry,
        boost::intrusive::member_hook<cell_entry, boost::intrusive::set_member_hook<>, &cell_entry::_link>,
        boost::intrusive::compare<cell_entry::compare>, boost::intrusive::constant_time_size<false>>;
public:
    static constexpr size_t max_vector_size = 32;
    static constexpr size_t internal_count = (sizeof(map_type) + sizeof(cell_entry)) / sizeof(atomic_cell_or_collection);
private:
    using vector_type = managed_vector<atomic_cell_or_collection, internal_count, size_type>;

    struct vector_storage {
        std::bitset<max_vector_size> present;
        vector_type v;
    };

    union storage {
        storage() { }
        ~storage() { }
        map_type set;
        vector_storage vector;
    } _storage;
public:
    row();
    ~row();
    row(const row&);
    row(row&& other) noexcept;
    row& operator=(row&& other) noexcept;
    size_t size() const { return _size; }
    bool empty() const { return _size == 0; }

    void reserve(column_id);

    const atomic_cell_or_collection& cell_at(column_id id) const;

    // Returns a pointer to cell's value or nullptr if column is not set.
    const atomic_cell_or_collection* find_cell(column_id id) const;
private:
    template<typename Func>
    void remove_if(Func&& func) {
        if (_type == storage_type::vector) {
            for (unsigned i = 0; i < _storage.vector.v.size(); i++) {
                if (!_storage.vector.present.test(i)) {
                    continue;
                }
                auto& c = _storage.vector.v[i];
                if (func(i, c)) {
                    c = atomic_cell_or_collection();
                    _storage.vector.present.reset(i);
                    _size--;
                }
            }
        } else {
            for (auto it = _storage.set.begin(); it != _storage.set.end();) {
                if (func(it->id(), it->cell())) {
                    auto& entry = *it;
                    it = _storage.set.erase(it);
                    current_allocator().destroy(&entry);
                    _size--;
                } else {
                    ++it;
                }
            }
        }
    }

private:
    auto get_range_vector() const {
        auto id_range = boost::irange<column_id>(0, _storage.vector.v.size());
        return boost::combine(id_range, _storage.vector.v)
        | boost::adaptors::filtered([this] (const boost::tuple<const column_id&, const atomic_cell_or_collection&>& t) {
            return _storage.vector.present.test(t.get<0>());
        }) | boost::adaptors::transformed([] (const boost::tuple<const column_id&, const atomic_cell_or_collection&>& t) {
            return std::pair<column_id, const atomic_cell_or_collection&>(t.get<0>(), t.get<1>());
        });
    }
    auto get_range_set() const {
        auto range = boost::make_iterator_range(_storage.set.begin(), _storage.set.end());
        return range | boost::adaptors::transformed([] (const cell_entry& c) {
            return std::pair<column_id, const atomic_cell_or_collection&>(c.id(), c.cell());
        });
    }
    template<typename Func>
    auto with_both_ranges(const row& other, Func&& func) const;

    void vector_to_set();

    // Calls Func(column_id, atomic_cell_or_collection&) for each cell in this row.
    //
    // Func() is allowed to modify the cell. Emptying a cell makes it still
    // visible to for_each().
    //
    // In case of exception, calls Rollback(column_id, atomic_cell_or_collection&) on
    // all cells on which Func() was successfully invoked in reverse order.
    //
    template<typename Func, typename Rollback>
    void for_each_cell(Func&&, Rollback&&);
public:
    // Calls Func(column_id, atomic_cell_or_collection&) for each cell in this row.
    // noexcept if Func doesn't throw.
    template<typename Func>
    void for_each_cell(Func&& func) {
        if (_type == storage_type::vector) {
            for (auto i : bitsets::for_each_set(_storage.vector.present)) {
                func(i, _storage.vector.v[i]);
            }
        } else {
            for (auto& cell : _storage.set) {
                func(cell.id(), cell.cell());
            }
        }
    }

    template<typename Func>
    void for_each_cell(Func&& func) const {
        for_each_cell_until([func = std::forward<Func>(func)] (column_id id, const atomic_cell_or_collection& c) {
            func(id, c);
            return stop_iteration::no;
        });
    }

    template<typename Func>
    void for_each_cell_until(Func&& func) const {
        if (_type == storage_type::vector) {
            for (auto i : bitsets::for_each_set(_storage.vector.present)) {
                auto& cell = _storage.vector.v[i];
                if (func(i, cell) == stop_iteration::yes) {
                    break;
                }
            }
        } else {
            for (auto& cell : _storage.set) {
                const auto& c = cell.cell();
                if (func(cell.id(), c) == stop_iteration::yes) {
                    break;
                }
            }
        }
    }

    // Merges cell's value into the row.
    void apply(const column_definition& column, const atomic_cell_or_collection& cell);

    //
    // Merges cell's value into the row.
    //
    // In case of exception the current object is left with a value equivalent to the original state.
    //
    // The external cell is left in a valid state, such that it will commute with
    // current object to the same value should the exception had not occurred.
    //
    void apply(const column_definition& column, atomic_cell_or_collection&& cell);

    // Equivalent to calling apply_reversibly() with a row containing only given cell.
    // See reversibly_mergeable.hh
    void apply_reversibly(const column_definition& column, atomic_cell_or_collection& cell);
    // See reversibly_mergeable.hh
    void revert(const column_definition& column, atomic_cell_or_collection& cell) noexcept;

    // Adds cell to the row. The column must not be already set.
    void append_cell(column_id id, atomic_cell_or_collection cell);

    void apply(const schema&, column_kind, const row& src);
    void apply(const schema&, column_kind, row&& src);

    // See reversibly_mergeable.hh
    void apply_reversibly(const schema&, column_kind, row& src);
    // See reversibly_mergeable.hh
    void revert(const schema&, column_kind, row& src) noexcept;

    // Expires cells based on query_time. Expires tombstones based on gc_before
    // and max_purgeable. Removes cells covered by tomb.
    // Returns true iff there are any live cells left.
    bool compact_and_expire(const schema& s, column_kind kind, row_tombstone tomb, gc_clock::time_point query_time,
        can_gc_fn&, gc_clock::time_point gc_before);

    row difference(const schema&, column_kind, const row& other) const;

    // Assumes the other row has the same schema
    // Consistent with feed_hash()
    bool operator==(const row&) const;

    bool equal(column_kind kind, const schema& this_schema, const row& other, const schema& other_schema) const;

    size_t external_memory_usage() const;

    friend std::ostream& operator<<(std::ostream& os, const row& r);
};

std::ostream& operator<<(std::ostream& os, const std::pair<column_id, const atomic_cell_or_collection&>& c);

class row_marker;
int compare_row_marker_for_merge(const row_marker& left, const row_marker& right) noexcept;

class row_marker {
    static constexpr gc_clock::duration no_ttl { 0 };
    static constexpr gc_clock::duration dead { -1 };
    api::timestamp_type _timestamp = api::missing_timestamp;
    gc_clock::duration _ttl = no_ttl;
    gc_clock::time_point _expiry;
public:
    row_marker() = default;
    explicit row_marker(api::timestamp_type created_at) : _timestamp(created_at) { }
    row_marker(api::timestamp_type created_at, gc_clock::duration ttl, gc_clock::time_point expiry)
        : _timestamp(created_at), _ttl(ttl), _expiry(expiry)
    { }
    explicit row_marker(tombstone deleted_at)
        : _timestamp(deleted_at.timestamp), _ttl(dead), _expiry(deleted_at.deletion_time)
    { }
    bool is_missing() const {
        return _timestamp == api::missing_timestamp;
    }
    bool is_live() const {
        return !is_missing() && _ttl != dead;
    }
    bool is_live(tombstone t, gc_clock::time_point now) const {
        if (is_missing() || _ttl == dead) {
            return false;
        }
        if (_ttl != no_ttl && _expiry < now) {
            return false;
        }
        return _timestamp > t.timestamp;
    }
    // Can be called only when !is_missing().
    bool is_dead(gc_clock::time_point now) const {
        if (_ttl == dead) {
            return true;
        }
        return _ttl != no_ttl && _expiry < now;
    }
    // Can be called only when is_live().
    bool is_expiring() const {
        return _ttl != no_ttl;
    }
    // Can be called only when is_expiring().
    gc_clock::duration ttl() const {
        return _ttl;
    }
    // Can be called only when is_expiring().
    gc_clock::time_point expiry() const {
        return _expiry;
    }
    // Can be called only when is_dead().
    gc_clock::time_point deletion_time() const {
        return _ttl == dead ? _expiry : _expiry - _ttl;
    }
    api::timestamp_type timestamp() const {
        return _timestamp;
    }
    void apply(const row_marker& rm) {
        if (compare_row_marker_for_merge(*this, rm) < 0) {
            *this = rm;
        }
    }
    // See reversibly_mergeable.hh
    void apply_reversibly(row_marker& rm) noexcept;
    // See reversibly_mergeable.hh
    void revert(row_marker& rm) noexcept;
    // Expires cells and tombstones. Removes items covered by higher level
    // tombstones.
    // Returns true if row marker is live.
    bool compact_and_expire(tombstone tomb, gc_clock::time_point now,
            can_gc_fn& can_gc, gc_clock::time_point gc_before) {
        if (is_missing()) {
            return false;
        }
        if (_timestamp <= tomb.timestamp) {
            _timestamp = api::missing_timestamp;
            return false;
        }
        if (_ttl > no_ttl && _expiry < now) {
            _expiry -= _ttl;
            _ttl = dead;
        }
        if (_ttl == dead && _expiry < gc_before && can_gc(tombstone(_timestamp, _expiry))) {
            _timestamp = api::missing_timestamp;
        }
        return !is_missing() && _ttl != dead;
    }
    // Consistent with feed_hash()
    bool operator==(const row_marker& other) const {
        if (_timestamp != other._timestamp) {
            return false;
        }
        if (is_missing()) {
            return true;
        }
        if (_ttl != other._ttl) {
            return false;
        }
        return _ttl == no_ttl || _expiry == other._expiry;
    }
    bool operator!=(const row_marker& other) const {
        return !(*this == other);
    }
    // Consistent with operator==()
    template<typename Hasher>
    void feed_hash(Hasher& h) const {
        ::feed_hash(h, _timestamp);
        if (!is_missing()) {
            ::feed_hash(h, _ttl);
            if (_ttl != no_ttl) {
                ::feed_hash(h, _expiry);
            }
        }
    }
    friend std::ostream& operator<<(std::ostream& os, const row_marker& rm);
};

template<>
struct appending_hash<row_marker> {
    template<typename Hasher>
    void operator()(Hasher& h, const row_marker& m) const {
        m.feed_hash(h);
    }
};

class clustering_row;

class shadowable_tombstone : public with_relational_operators<shadowable_tombstone> {
    tombstone _tomb;
public:

    explicit shadowable_tombstone(api::timestamp_type timestamp, gc_clock::time_point deletion_time)
            : _tomb(timestamp, deletion_time) {
    }

    explicit shadowable_tombstone(tombstone tomb = tombstone())
            : _tomb(std::move(tomb)) {
    }

    int compare(const shadowable_tombstone& t) const {
        return _tomb.compare(t._tomb);
    }

    explicit operator bool() const {
        return bool(_tomb);
    }

    const tombstone& tomb() const {
        return _tomb;
    }

    // A shadowable row tombstone is valid only if the row has no live marker. In other words,
    // the row tombstone is only valid as long as no newer insert is done (thus setting a
    // live row marker; note that if the row timestamp set is lower than the tombstone's,
    // then the tombstone remains in effect as usual). If a row has a shadowable tombstone
    // with timestamp Ti and that row is updated with a timestamp Tj, such that Tj > Ti
    // (and that update sets the row marker), then the shadowable tombstone is shadowed by
    // that update. A concrete consequence is that if the update has cells with timestamp
    // lower than Ti, then those cells are preserved (since the deletion is removed), and
    // this is contrary to a regular, non-shadowable row tombstone where the tombstone is
    // preserved and such cells are removed.
    bool is_shadowed_by(const row_marker& marker) const {
        return marker.is_live() && marker.timestamp() > _tomb.timestamp;
    }

    void maybe_shadow(tombstone t, row_marker marker) noexcept {
        if (is_shadowed_by(marker)) {
            _tomb = std::move(t);
        }
    }

    void apply(tombstone t) noexcept {
        _tomb.apply(t);
    }

    void apply(shadowable_tombstone t) noexcept {
        _tomb.apply(t._tomb);
    }

    friend std::ostream& operator<<(std::ostream& out, const shadowable_tombstone& t) {
        if (t) {
            return out << "{shadowable tombstone: timestamp=" << t.tomb().timestamp
                   << ", deletion_time=" << t.tomb().deletion_time.time_since_epoch().count()
                   << "}";
        } else {
            return out << "{shadowable tombstone: none}";
        }
    }
};

template<>
struct appending_hash<shadowable_tombstone> {
    template<typename Hasher>
    void operator()(Hasher& h, const shadowable_tombstone& t) const {
        feed_hash(h, t.tomb());
    }
};

/*
The rules for row_tombstones are as follows:
  - The shadowable tombstone is always >= than the regular one;
  - The regular tombstone works as expected;
  - The shadowable tombstone doesn't erase or compact away the regular
    row tombstone, nor dead cells;
  - The shadowable tombstone can erase live cells, but only provided they
    can be recovered (e.g., by including all cells in a MV update, both
    updated cells and pre-existing ones);
  - The shadowable tombstone can be erased or compacted away by a newer
    row marker.
*/
class row_tombstone : public with_relational_operators<row_tombstone> {
    tombstone _regular;
    shadowable_tombstone _shadowable; // _shadowable is always >= _regular
public:
    explicit row_tombstone(tombstone regular, shadowable_tombstone shadowable)
            : _regular(std::move(regular))
            , _shadowable(std::move(shadowable)) {
    }

    explicit row_tombstone(tombstone regular)
            : row_tombstone(regular, shadowable_tombstone(regular)) {
    }

    row_tombstone() = default;

    int compare(const row_tombstone& t) const {
        return _shadowable.compare(t._shadowable);
    }

    explicit operator bool() const {
        return bool(_shadowable);
    }

    const tombstone& tomb() const {
        return _shadowable.tomb();
    }

    const gc_clock::time_point max_deletion_time() const {
        return std::max(_regular.deletion_time, _shadowable.tomb().deletion_time);
    }

    const tombstone& regular() const {
        return _regular;
    }

    const shadowable_tombstone& shadowable() const {
        return _shadowable;
    }

    bool is_shadowable() const {
        return _shadowable.tomb() > _regular;
    }

    void maybe_shadow(const row_marker& marker) noexcept {
        _shadowable.maybe_shadow(_regular, marker);
    }

    void apply(tombstone regular) noexcept {
        _shadowable.apply(regular);
        _regular.apply(regular);
    }

    void apply(shadowable_tombstone shadowable, row_marker marker) noexcept {
        _shadowable.apply(shadowable.tomb());
        _shadowable.maybe_shadow(_regular, marker);
    }

    void apply(row_tombstone t, row_marker marker) noexcept {
        _regular.apply(t._regular);
        _shadowable.apply(t._shadowable);
        _shadowable.maybe_shadow(_regular, marker);
    }

    // See reversibly_mergeable.hh
    void apply_reversibly(row_tombstone& t, row_marker marker) noexcept {
        std::swap(*this, t);
        apply(t, marker);
    }

    // See reversibly_mergeable.hh
    void revert(row_tombstone& t) noexcept {
        std::swap(*this, t);
    }

    friend std::ostream& operator<<(std::ostream& out, const row_tombstone& t) {
        if (t) {
            return out << "{row_tombstone: " << t._regular << (t.is_shadowable() ? t._shadowable : shadowable_tombstone()) << "}";
        } else {
            return out << "{row_tombstone: none}";
        }
    }
};

template<>
struct appending_hash<row_tombstone> {
    template<typename Hasher>
    void operator()(Hasher& h, const row_tombstone& t) const {
        feed_hash(h, t.regular());
        if (t.is_shadowable()) {
            feed_hash(h, t.shadowable());
        }
    }
};

class deletable_row final {
    row_tombstone _deleted_at;
    row_marker _marker;
    row _cells;
public:
    deletable_row() {}
    explicit deletable_row(clustering_row&&);
    deletable_row(row_tombstone tomb, const row_marker& marker, const row& cells)
        : _deleted_at(tomb), _marker(marker), _cells(cells)
    {}

    void apply(tombstone deleted_at) {
        _deleted_at.apply(deleted_at);
    }

    void apply(shadowable_tombstone deleted_at) {
        _deleted_at.apply(deleted_at, _marker);
    }

    void apply(row_tombstone deleted_at) {
        _deleted_at.apply(deleted_at, _marker);
    }

    void apply(const row_marker& rm) {
        _marker.apply(rm);
        _deleted_at.maybe_shadow(_marker);
    }

    void remove_tombstone() {
        _deleted_at = {};
    }

    // See reversibly_mergeable.hh
    void apply_reversibly(const schema& s, deletable_row& src);
    // See reversibly_mergeable.hh
    void revert(const schema& s, deletable_row& src);

    // Weak exception guarantees. After exception, both src and this will commute to the same value as
    // they would should the exception not happen.
    void apply(const schema& s, deletable_row&& src);
public:
    row_tombstone deleted_at() const { return _deleted_at; }
    api::timestamp_type created_at() const { return _marker.timestamp(); }
    row_marker& marker() { return _marker; }
    const row_marker& marker() const { return _marker; }
    const row& cells() const { return _cells; }
    row& cells() { return _cells; }
    friend std::ostream& operator<<(std::ostream& os, const deletable_row& dr);
    bool equal(column_kind, const schema& s, const deletable_row& other, const schema& other_schema) const;
    bool is_live(const schema& s, tombstone base_tombstone = tombstone(), gc_clock::time_point query_time = gc_clock::time_point::min()) const;
    bool empty() const { return !_deleted_at && _marker.is_missing() && !_cells.size(); }
    deletable_row difference(const schema&, column_kind, const deletable_row& other) const;
};

class rows_entry {
    intrusive_set_external_comparator_member_hook _link;
    clustering_key _key;
    deletable_row _row;
    struct flags {
        bool _continuous : 1; // See doc of is_continuous.
        bool _dummy : 1;
        bool _last : 1;
        bool _erased : 1; // Used only temporarily during apply_reversibly(). Refs #2012.
        flags() : _continuous(true), _dummy(false), _last(false), _erased(false) { }
    } _flags{};
    friend class mutation_partition;
public:
    struct erased_tag {};
    rows_entry(erased_tag, const rows_entry& e)
        : _key(e._key)
    {
        _flags._erased = true;
        _flags._last = e._flags._last;
    }
    explicit rows_entry(clustering_key&& key)
        : _key(std::move(key))
    { }
    explicit rows_entry(const clustering_key& key)
        : _key(key)
    { }
    rows_entry(const schema& s, position_in_partition_view pos, is_dummy dummy, is_continuous continuous)
        : _key(pos.key())
    {
        if (!pos.is_clustering_row()) {
            assert(bool(dummy));
            assert(pos.is_after_all_clustered_rows(s)); // FIXME: Support insertion at any position
            _flags._last = true;
        }
        _flags._dummy = bool(dummy);
        _flags._continuous = bool(continuous);
    }
    rows_entry(const clustering_key& key, deletable_row&& row)
        : _key(key), _row(std::move(row))
    { }
    rows_entry(const clustering_key& key, const deletable_row& row)
        : _key(key), _row(row)
    { }
    rows_entry(const clustering_key& key, row_tombstone tomb, const row_marker& marker, const row& row)
        : _key(key), _row(tomb, marker, row)
    { }
    rows_entry(rows_entry&& o) noexcept;
    rows_entry(const rows_entry& e)
        : _key(e._key)
        , _row(e._row)
        , _flags(e._flags)
    { }
    // Valid only if !dummy()
    clustering_key& key() {
        return _key;
    }
    // Valid only if !dummy()
    const clustering_key& key() const {
        return _key;
    }
    deletable_row& row() {
        return _row;
    }
    const deletable_row& row() const {
        return _row;
    }
    position_in_partition_view position() const {
        if (_flags._last) {
            return position_in_partition_view::after_all_clustered_rows();
        } else {
            return position_in_partition_view(
                    position_in_partition_view::clustering_row_tag_t(), _key);
        }
    }

    is_continuous continuous() const { return is_continuous(_flags._continuous); }
    void set_continuous(bool value) { _flags._continuous = value; }
    void set_continuous(is_continuous value) { set_continuous(bool(value)); }
    is_dummy dummy() const { return is_dummy(_flags._dummy); }
    void apply(row_tombstone t) {
        _row.apply(t);
    }
    // See reversibly_mergeable.hh
    void apply_reversibly(const schema& s, rows_entry& e) {
        _row.apply_reversibly(s, e._row);
    }
    // See reversibly_mergeable.hh
    void revert(const schema& s, rows_entry& e) noexcept {
        _row.revert(s, e._row);
    }
    bool empty() const {
        return _row.empty();
    }
    bool erased() const {
        return _flags._erased;
    }
    struct tri_compare {
        position_in_partition::tri_compare _c;
        explicit tri_compare(const schema& s) : _c(s) {}
        int operator()(const rows_entry& e1, const rows_entry& e2) const {
            return _c(e1.position(), e2.position());
        }
        int operator()(const clustering_key& key, const rows_entry& e) const {
            return _c(position_in_partition_view::for_key(key), e.position());
        }
        int operator()(const rows_entry& e, const clustering_key& key) const {
            return _c(e.position(), position_in_partition_view::for_key(key));
        }
        int operator()(const rows_entry& e, position_in_partition_view p) const {
            return _c(e.position(), p);
        }
        int operator()(position_in_partition_view p, const rows_entry& e) const {
            return _c(p, e.position());
        }
        int operator()(position_in_partition_view p1, position_in_partition_view p2) const {
            return _c(p1, p2);
        }
    };
    struct compare {
        tri_compare _c;
        explicit compare(const schema& s) : _c(s) {}
        bool operator()(const rows_entry& e1, const rows_entry& e2) const {
            return _c(e1, e2) < 0;
        }
        bool operator()(const clustering_key& key, const rows_entry& e) const {
            return _c(key, e) < 0;
        }
        bool operator()(const rows_entry& e, const clustering_key& key) const {
            return _c(e, key) < 0;
        }
        bool operator()(const clustering_key_view& key, const rows_entry& e) const {
            return _c(key, e) < 0;
        }
        bool operator()(const rows_entry& e, const clustering_key_view& key) const {
            return _c(e, key) < 0;
        }
        bool operator()(const rows_entry& e, position_in_partition_view p) const {
            return _c(e.position(), p) < 0;
        }
        bool operator()(position_in_partition_view p, const rows_entry& e) const {
            return _c(p, e.position()) < 0;
        }
        bool operator()(position_in_partition_view p1, position_in_partition_view p2) const {
            return _c(p1, p2) < 0;
        }
    };
    template <typename Comparator>
    struct delegating_compare {
        Comparator _c;
        delegating_compare(Comparator&& c) : _c(std::move(c)) {}
        template <typename Comparable>
        bool operator()(const Comparable& v, const rows_entry& e) const {
            if (e._flags._last) {
                return true;
            }
            return _c(v, e._key);
        }
        template <typename Comparable>
        bool operator()(const rows_entry& e, const Comparable& v) const {
            if (e._flags._last) {
                return false;
            }
            return _c(e._key, v);
        }
    };
    template <typename Comparator>
    static auto key_comparator(Comparator&& c) {
        return delegating_compare<Comparator>(std::move(c));
    }
    friend std::ostream& operator<<(std::ostream& os, const rows_entry& re);
    bool equal(const schema& s, const rows_entry& other) const;
    bool equal(const schema& s, const rows_entry& other, const schema& other_schema) const;
};

// Represents a set of writes made to a single partition.
//
// The object is schema-dependent. Each instance is governed by some
// specific schema version. Accessors require a reference to the schema object
// of that version.
//
// There is an operation of addition defined on mutation_partition objects
// (also called "apply"), which gives as a result an object representing the
// sum of writes contained in the addends. For instances governed by the same
// schema, addition is commutative and associative.
//
// In addition to representing writes, the object supports specifying a set of
// partition elements called "continuity". This set can be used to represent
// lack of information about certain parts of the partition. It can be
// specified which ranges of clustering keys belong to that set. We say that a
// key range is continuous if all keys in that range belong to the continuity
// set, and discontinuous otherwise. By default everything is continuous.
// The static row may be also continuous or not.
// Partition tombstone is always continuous.
//
// Continuity is ignored by instance equality. It's also transient, not
// preserved by serialization.
//
// Continuity is represented internally using flags on row entries. The key
// range between two consecutive entries (both ends exclusive) is continuous
// if and only if rows_entry::continuous() is true for the later entry. The
// range starting after the last entry is assumed to be continuous. The range
// corresponding to the key of the entry is continuous if and only if
// rows_entry::dummy() is false.
//
// Adding two fully-continuous instances gives a fully-continuous instance.
// Continuity doesn't affect how the write part is added.
//
// Addition of continuity is not commutative in general, but is associative.
// Continuity flags on objects representing the same thing (e.g. rows_entry
// with the same key) are merged such that the information stored in the left-
// hand operand wins. Flags on objects which are present only in one of the
// operands are transferred as-is. Such merging rules are useful for layering
// information in MVCC, where newer versions specify continuity with respect
// to the combined set of rows in all prior versions, not just in their
// versions.
class mutation_partition final {
public:
    using rows_type = intrusive_set_external_comparator<rows_entry, &rows_entry::_link>;
    friend class rows_entry;
    friend class size_calculator;
private:
    tombstone _tombstone;
    row _static_row;
    bool _static_row_continuous = true;
    rows_type _rows;
    // Contains only strict prefixes so that we don't have to lookup full keys
    // in both _row_tombstones and _rows.
    range_tombstone_list _row_tombstones;

    friend class mutation_partition_applier;
    friend class converting_mutation_partition_applier;

    bool check_continuity(const schema&, const position_range&, is_continuous);
public:
    struct copy_comparators_only {};
    struct incomplete_tag {};
    // Constructs an empty instance which is fully discontinuous except for the partition tombstone.
    mutation_partition(incomplete_tag, const schema& s, tombstone);
    static mutation_partition make_incomplete(const schema& s, tombstone t = {}) {
        return mutation_partition(incomplete_tag(), s, t);
    }
    mutation_partition(schema_ptr s)
        : _rows()
        , _row_tombstones(*s)
    { }
    mutation_partition(mutation_partition& other, copy_comparators_only)
        : _rows()
        , _row_tombstones(other._row_tombstones, range_tombstone_list::copy_comparator_only())
    { }
    mutation_partition(mutation_partition&&) = default;
    mutation_partition(const mutation_partition&);
    mutation_partition(const mutation_partition&, const schema&, query::clustering_key_filter_ranges);
    mutation_partition(mutation_partition&&, const schema&, query::clustering_key_filter_ranges);
    ~mutation_partition();
    mutation_partition& operator=(const mutation_partition& x);
    mutation_partition& operator=(mutation_partition&& x) noexcept;
    bool equal(const schema&, const mutation_partition&) const;
    bool equal(const schema& this_schema, const mutation_partition& p, const schema& p_schema) const;
    bool equal_continuity(const schema&, const mutation_partition&) const;
    // Consistent with equal()
    template<typename Hasher>
    void feed_hash(Hasher& h, const schema& s) const {
        hashing_partition_visitor<Hasher> v(h, s);
        accept(s, v);
    }
    friend std::ostream& operator<<(std::ostream& os, const mutation_partition& mp);
public:
    // Makes sure there is a dummy entry after all clustered rows. Doesn't affect continuity.
    // Doesn't invalidate iterators.
    void ensure_last_dummy(const schema&);
    bool static_row_continuous() const { return _static_row_continuous; }
    void set_static_row_continuous(bool value) { _static_row_continuous = value; }
    bool is_fully_continuous() const;
    void make_fully_continuous();
    // Returns true iff all keys from given range are marked as continuous, or range is empty.
    bool fully_continuous(const schema&, const position_range&);
    // Returns true iff all keys from given range are marked as not continuous and range is not empty.
    bool fully_discontinuous(const schema&, const position_range&);
    // Removes all data, marking affected ranges as discontinuous.
    void evict() noexcept;
    void apply(tombstone t) { _tombstone.apply(t); }
    void apply_delete(const schema& schema, const clustering_key_prefix& prefix, tombstone t);
    void apply_delete(const schema& schema, range_tombstone rt);
    void apply_delete(const schema& schema, clustering_key_prefix&& prefix, tombstone t);
    void apply_delete(const schema& schema, clustering_key_prefix_view prefix, tombstone t);
    // Equivalent to applying a mutation with an empty row, created with given timestamp
    void apply_insert(const schema& s, clustering_key_view, api::timestamp_type created_at);
    // prefix must not be full
    void apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t);
    void apply_row_tombstone(const schema& schema, range_tombstone rt);
    //
    // Applies p to current object.
    //
    // Commutative when this_schema == p_schema. If schemas differ, data in p which
    // is not representable in this_schema is dropped, thus apply() loses commutativity.
    //
    // Strong exception guarantees.
    void apply(const schema& this_schema, const mutation_partition& p, const schema& p_schema);
    //
    // Applies p to current object.
    //
    // Commutative when this_schema == p_schema. If schemas differ, data in p which
    // is not representable in this_schema is dropped, thus apply() loses commutativity.
    //
    // If exception is thrown, this object will be left in a state equivalent to the entry state
    // and p will be left in a state which will commute with current object to the same value
    // should the exception had not occurred.
    void apply(const schema& this_schema, mutation_partition&& p, const schema& p_schema);
    // Use in case this instance and p share the same schema.
    // Same guarantees as apply(const schema&, mutation_partition&&, const schema&);
    void apply(const schema& s, mutation_partition&& p);
    // Same guarantees and constraints as for apply(const schema&, const mutation_partition&, const schema&).
    void apply(const schema& this_schema, mutation_partition_view p, const schema& p_schema);

    // Converts partition to the new schema. When succeeds the partition should only be accessed
    // using the new schema.
    //
    // Strong exception guarantees.
    void upgrade(const schema& old_schema, const schema& new_schema);
private:
    void insert_row(const schema& s, const clustering_key& key, deletable_row&& row);
    void insert_row(const schema& s, const clustering_key& key, const deletable_row& row);

    uint32_t do_compact(const schema& s,
        gc_clock::time_point now,
        const std::vector<query::clustering_range>& row_ranges,
        bool reverse,
        uint32_t row_limit,
        can_gc_fn&);

    // Calls func for each row entry inside row_ranges until func returns stop_iteration::yes.
    // Removes all entries for which func didn't return stop_iteration::no or wasn't called at all.
    // Removes all entries that are empty, check rows_entry::empty().
    // If reversed is true, func will be called on entries in reverse order. In that case row_ranges
    // must be already in reverse order.
    template<bool reversed, typename Func>
    void trim_rows(const schema& s,
        const std::vector<query::clustering_range>& row_ranges,
        Func&& func);
public:
    // Performs the following:
    //   - throws out data which doesn't belong to row_ranges
    //   - expires cells and tombstones based on query_time
    //   - drops cells covered by higher-level tombstones (compaction)
    //   - leaves at most row_limit live rows
    //
    // Note: a partition with a static row which has any cell live but no
    // clustered rows still counts as one row, according to the CQL row
    // counting rules.
    //
    // Returns the count of CQL rows which remained. If the returned number is
    // smaller than the row_limit it means that there was no more data
    // satisfying the query left.
    //
    // The row_limit parameter must be > 0.
    //
    uint32_t compact_for_query(const schema& s, gc_clock::time_point query_time,
        const std::vector<query::clustering_range>& row_ranges, bool reversed, uint32_t row_limit);

    // Performs the following:
    //   - expires cells based on compaction_time
    //   - drops cells covered by higher-level tombstones
    //   - drops expired tombstones which timestamp is before max_purgeable
    void compact_for_compaction(const schema& s, can_gc_fn&,
        gc_clock::time_point compaction_time);

    // Returns the minimal mutation_partition that when applied to "other" will
    // create a mutation_partition equal to the sum of other and this one.
    // This and other must both be governed by the same schema s.
    mutation_partition difference(schema_ptr s, const mutation_partition& other) const;

    // Returns true if there is no live data or tombstones.
    bool empty() const;
public:
    deletable_row& clustered_row(const schema& s, const clustering_key& key);
    deletable_row& clustered_row(const schema& s, clustering_key&& key);
    deletable_row& clustered_row(const schema& s, clustering_key_view key);
    deletable_row& clustered_row(const schema& s, position_in_partition_view pos, is_dummy, is_continuous);
public:
    tombstone partition_tombstone() const { return _tombstone; }
    row& static_row() { return _static_row; }
    const row& static_row() const { return _static_row; }
    // return a set of rows_entry where each entry represents a CQL row sharing the same clustering key.
    const rows_type& clustered_rows() const { return _rows; }
    const range_tombstone_list& row_tombstones() const { return _row_tombstones; }
    rows_type& clustered_rows() { return _rows; }
    range_tombstone_list& row_tombstones() { return _row_tombstones; }
    const row* find_row(const schema& s, const clustering_key& key) const;
    tombstone range_tombstone_for_row(const schema& schema, const clustering_key& key) const;
    row_tombstone tombstone_for_row(const schema& schema, const clustering_key& key) const;
    // Can be called only for non-dummy entries
    row_tombstone tombstone_for_row(const schema& schema, const rows_entry& e) const;
    boost::iterator_range<rows_type::const_iterator> range(const schema& schema, const query::clustering_range& r) const;
    rows_type::const_iterator lower_bound(const schema& schema, const query::clustering_range& r) const;
    rows_type::const_iterator upper_bound(const schema& schema, const query::clustering_range& r) const;
    rows_type::iterator lower_bound(const schema& schema, const query::clustering_range& r);
    rows_type::iterator upper_bound(const schema& schema, const query::clustering_range& r);
    boost::iterator_range<rows_type::iterator> range(const schema& schema, const query::clustering_range& r);
    // Returns an iterator range of rows_entry, with only non-dummy entries.
    auto non_dummy_rows() const {
        return boost::make_iterator_range(_rows.begin(), _rows.end())
            | boost::adaptors::filtered([] (const rows_entry& e) { return bool(!e.dummy()); });
    }
    // Writes this partition using supplied query result writer.
    // The partition should be first compacted with compact_for_query(), otherwise
    // results may include data which is deleted/expired.
    // At most row_limit CQL rows will be written and digested.
    void query_compacted(query::result::partition_writer& pw, const schema& s, uint32_t row_limit) const;
    void accept(const schema&, mutation_partition_visitor&) const;

    // Returns the number of live CQL rows in this partition.
    //
    // Note: If no regular rows are live, but there's something live in the
    // static row, the static row counts as one row. If there is at least one
    // regular row live, static row doesn't count.
    //
    size_t live_row_count(const schema&,
        gc_clock::time_point query_time = gc_clock::time_point::min()) const;

    bool is_static_row_live(const schema&,
        gc_clock::time_point query_time = gc_clock::time_point::min()) const;
private:
    template<typename Func>
    void for_each_row(const schema& schema, const query::clustering_range& row_range, bool reversed, Func&& func) const;
    friend class counter_write_query_result_builder;
};
