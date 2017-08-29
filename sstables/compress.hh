/*
 * Copyright (C) 2015 ScyllaDB
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

// This is an implementation of a random-access compressed file compatible
// with Cassandra's org.apache.cassandra.io.compress compressed files.
//
// To allow reasonably-efficient seeking in the compressed file, the file
// is not compressed as a whole, but rather divided into chunks of a known
// size (by default, 64 KB), where each chunk is compressed individually.
// The compressed size of each chunk is different, so for allowing seeking
// to a particular position in the uncompressed data, we need to also know
// the position of each chunk. This offset vector is supplied externally as
// a "compression_metadata" object, which also contains additional information
// needed from decompression - such as the chunk size and compressor type.
//
// Cassandra supports three different compression algorithms for the chunks,
// LZ4, Snappy, and Deflate - the default (and therefore most important) is
// LZ4. Each compressor is an implementation of the "compressor" class.
//
// Each compressed chunk is followed by a 4-byte checksum of the compressed
// data, using the Adler32 algorithm. In Cassandra, there is a parameter
// "crc_check_chance" (defaulting to 1.0) which determines the probability
// of us verifying the checksum of each chunk we read.
//
// This implementation does not cache the compressed disk blocks (which
// are read using O_DIRECT), nor uncompressed data. We intend to cache high-
// level Cassandra rows, not disk blocks.

#include <vector>
#include <cstdint>
#include <iterator>
#include <zlib.h>

#include "core/file.hh"
#include "core/reactor.hh"
#include "core/shared_ptr.hh"
#include "types.hh"
#include "../compress.hh"

// An "uncompress_func" is a function which uncompresses the given compressed
// input chunk, and writes the uncompressed data into the given output buffer.
// An exception is thrown if the output buffer is not big enough, but that
// is not expected to happen - in the chunked compression scheme used here,
// we know that the uncompressed data will be exactly chunk_size bytes (or
// smaller for the last chunk).
typedef size_t uncompress_func(const char* input, size_t input_len,
        char* output, size_t output_len);

uncompress_func uncompress_lz4;
uncompress_func uncompress_snappy;
uncompress_func uncompress_deflate;

typedef size_t compress_func(const char* input, size_t input_len,
        char* output, size_t output_len);

compress_func compress_lz4;
compress_func compress_snappy;
compress_func compress_deflate;

typedef size_t compress_max_size_func(size_t input_len);

compress_max_size_func compress_max_size_lz4;
compress_max_size_func compress_max_size_snappy;
compress_max_size_func compress_max_size_deflate;

inline uint32_t init_checksum_adler32() {
    return adler32(0, Z_NULL, 0);
}

inline uint32_t checksum_adler32(const char* input, size_t input_len) {
    auto init = adler32(0, Z_NULL, 0);
    // yuck, zlib uses unsigned char while we use char :-(
    return adler32(init, reinterpret_cast<const unsigned char *>(input),
            input_len);
}

inline uint32_t checksum_adler32(uint32_t adler, const char* input, size_t input_len) {
    return adler32(adler, reinterpret_cast<const unsigned char *>(input),
            input_len);
}

inline uint32_t checksum_adler32_combine(uint32_t adler1, uint32_t adler2, size_t input_len2) {
    return adler32_combine(adler1, adler2, input_len2);
}

namespace sstables {

struct compression {
    // To reduce the memory footpring of compression-info, n offsets are grouped
    // together into segments, where each segment stores a base absolute offset
    // into the file, the other offsets in the segments being relative offsets
    // (and thus of reduced size). Also offsets are allocated only just enough
    // bits to store their maximum value. The offsets are thus packed in a
    // buffer like so:
    //      arrrarrrarrr...
    // where n is 4, a is an absolute offset and r are offsets relative to a.
    // Segments are stored in buckets, where each bucket has its own base offset.
    // Segments in a buckets are optimized to address as large of a chunk of the
    // data as possible for a given chunk size and bucket size.
    //
    // This is not a general purpose container. There are limitations:
    // * Can't be used before init() is called.
    // * at() is best called incrementally, altough random lookups are
    // perfectly valid as well.
    // * The iterator and at() can't provide references to the elements.
    // * No point insert is available.
    class segmented_offsets {
        struct bucket {
            uint64_t base_offset;
            std::unique_ptr<char[]> storage;
        };

        uint32_t _chunk_size{0};
        uint8_t _segment_base_offset_size_bits{0};
        uint8_t _segmented_offset_size_bits{0};
        uint16_t _segment_size_bits{0};
        uint32_t _segments_per_bucket{0};
        uint8_t _grouped_offsets{0};

        mutable std::size_t _current_index{0};
        mutable std::size_t _current_bucket_index{0};
        mutable uint64_t _current_bucket_segment_index{0};
        mutable uint64_t _current_segment_relative_index{0};
        mutable uint64_t _current_segment_offset_bits{0};

        uint64_t _last_written_offset{0};

        std::size_t _size{0};
        std::deque<bucket> _storage;

        uint64_t read(uint64_t bucket_index, uint64_t offset_bits, uint64_t size_bits) const;
        void write(uint64_t bucket_index, uint64_t offset_bits, uint64_t size_bits, uint64_t value);

        void update_position_trackers(std::size_t index) const;

    public:
        class const_iterator : public std::iterator<std::random_access_iterator_tag, const uint64_t> {
            friend class segmented_offsets;
            struct end_tag {};

            const segmented_offsets& _offsets;
            std::size_t _index;

            const_iterator(const segmented_offsets& offsets)
                : _offsets(offsets)
                , _index(0) {
            }

            const_iterator(const segmented_offsets& offsets, end_tag)
                : _offsets(offsets)
                , _index(_offsets.size()) {
            }

        public:
            const_iterator(const const_iterator& other) = default;

            const_iterator& operator=(const const_iterator& other) {
                assert(&_offsets == &other._offsets);
                _index = other._index;
                return *this;
            }

            const_iterator operator++(int) {
                const_iterator it{*this};
                return ++it;
            }

            const_iterator& operator++() {
                *this += 1;
                return *this;
            }

            const_iterator operator+(ssize_t i) const {
                const_iterator it{*this};
                it += i;
                return it;
            }

            const_iterator& operator+=(ssize_t i) {
                _index += i;

                return *this;
            }

            const_iterator operator--(int) {
                const_iterator it{*this};
                return --it;
            }

            const_iterator& operator--() {
                *this -= 1;
                return *this;
            }

            const_iterator operator-(ssize_t i) const {
                const_iterator it{*this};
                it -= i;
                return it;
            }

            const_iterator& operator-=(ssize_t i) {
                _index -= i;
                return *this;
            }

            value_type operator*() const {
                return _offsets.at(_index);
            }

            value_type operator[](ssize_t i) const {
                return _offsets.at(_index + i);
            }

            bool operator==(const const_iterator& other) const {
                return _index == other._index;
            }

            bool operator!=(const const_iterator& other) const {
                return !(*this == other);
            }

            bool operator<(const const_iterator& other) const {
                return _index < other._index;
            }

            bool operator<=(const const_iterator& other) const {

                return _index <= other._index;
            }

            bool operator>(const const_iterator& other) const {
                return _index > other._index;
            }

            bool operator>=(const const_iterator& other) const {
                return _index >= other._index;
            }
        };

        segmented_offsets() = default;

        segmented_offsets(const segmented_offsets&) = delete;
        segmented_offsets& operator=(const segmented_offsets&) = delete;

        segmented_offsets(segmented_offsets&&) = default;
        segmented_offsets& operator=(segmented_offsets&&) = default;

        // Has to be called before using the class. Doing otherwise
        // results in undefined behaviour! Don't call more than once!
        // TODO: fold into constructor, once the parse() et. al. code
        // allows it.
        void init(uint32_t chunk_size);

        uint32_t chunk_size() const noexcept {
            return _chunk_size;
        }

        std::size_t size() const noexcept {
            return _size;
        }

        uint64_t at(std::size_t i) const;

        void push_back(uint64_t offset);

        const_iterator begin() const {
            return const_iterator(*this);
        }

        const_iterator end() const {
            return const_iterator(*this, const_iterator::end_tag{});
        }

        const_iterator cbegin() const {
            return const_iterator(*this);
        }

        const_iterator cend() const {
            return const_iterator(*this, const_iterator::end_tag{});
        }
    };

    disk_string<uint16_t> name;
    disk_array<uint32_t, option> options;
    uint32_t chunk_len;
    uint64_t data_len;
    segmented_offsets offsets;

private:
    // Variables determined from the above deserialized values, held for convenience:
    uncompress_func *_uncompress = nullptr;
    compress_func *_compress = nullptr;
    // Return maximum length of data that compressor may output.
    compress_max_size_func *_compress_max_size = nullptr;
    // Variables *not* found in the "Compression Info" file (added by update()):
    uint64_t _compressed_file_length = 0;
    uint32_t _full_checksum;
public:
    // Set the compressor algorithm, please check the definition of enum compressor.
    void set_compressor(compressor c);
    // After changing _compression, update() must be called to update
    // additional variables depending on it.
    void update(uint64_t compressed_file_length);
    operator bool() const {
        return _uncompress != nullptr;
    }
    // locate() locates in the compressed file the given byte position of
    // the uncompressed data:
    //   1. The byte range containing the appropriate compressed chunk, and
    //   2. the offset into the uncompressed chunk.
    // Note that the last 4 bytes of the returned chunk are not the actual
    // compressed data, but rather the checksum of the compressed data.
    // locate() throws an out-of-range exception if the position is beyond
    // the last chunk.
    struct chunk_and_offset {
        uint64_t chunk_start;
        uint64_t chunk_len; // variable size of compressed chunk
        unsigned offset; // offset into chunk after uncompressing it
    };
    chunk_and_offset locate(uint64_t position) const;

    unsigned uncompressed_chunk_length() const noexcept {
        return chunk_len;
    }

    void set_uncompressed_chunk_length(uint32_t cl) {
        chunk_len = cl;

        offsets.init(chunk_len);
    }

    uint64_t uncompressed_file_length() const noexcept {
        return data_len;
    }

    void set_uncompressed_file_length(uint64_t fl) {
        data_len = fl;
    }

    uint64_t compressed_file_length() const {
        return _compressed_file_length;
    }
    void set_compressed_file_length(uint64_t compressed_file_length) {
        _compressed_file_length = compressed_file_length;
    }

    uint32_t full_checksum() const {
        return _full_checksum;
    }
    void init_full_checksum() {
        _full_checksum = init_checksum_adler32();
    }
    void update_full_checksum(uint32_t checksum, size_t size) {
        _full_checksum = checksum_adler32_combine(_full_checksum, checksum, size);
    }

    size_t uncompress(
            const char* input, size_t input_len,
            char* output, size_t output_len) const {
        if (!_uncompress) {
            throw std::runtime_error("uncompress is not supported");
        }
        return _uncompress(input, input_len, output, output_len);
    }
    size_t compress(
            const char* input, size_t input_len,
            char* output, size_t output_len) const {
        if (!_compress) {
            throw std::runtime_error("compress is not supported");
        }
        return _compress(input, input_len, output, output_len);
    }
    size_t compress_max_size(size_t input_len) const {
        return _compress_max_size(input_len);
    }
    friend class sstable;
};

}


// Note: compression_metadata is passed by reference; The caller is
// responsible for keeping the compression_metadata alive as long as there
// are open streams on it. This should happen naturally on a higher level -
// as long as we have *sstables* work in progress, we need to keep the whole
// sstable alive, and the compression metadata is only a part of it.
input_stream<char> make_compressed_file_input_stream(
        file f, sstables::compression *cm, uint64_t offset, size_t len, class file_input_stream_options options);
