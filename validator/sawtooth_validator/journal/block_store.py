# Copyright 2016 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------


# pylint: disable=no-name-in-module
from collections.abc import MutableMapping
from sawtooth_validator.journal.block_wrapper import BlockStatus
from sawtooth_validator.journal.block_wrapper import BlockWrapper


class BlockStore(MutableMapping):
    """
    A dict like interface wrapper around the block store to guarantee,
    objects are correctly wrapped and unwrapped as they are stored and
    retrieved.
    """
    def __init__(self, block_db):
        self._block_store = block_db

    def __setitem__(self, key, value):
        if key != value.identifier:
            raise KeyError("Invalid key to store block under: {} expected {}".\
                           format(key, value.identifier))
        add_ops = self._build_add_block_ops(value)
        self._block_store.set_batch(add_ops)

    def __getitem__(self, key):
        block = self._block_store[key]
        if block is not None:
            return BlockWrapper(
                status=BlockStatus.Valid,
                **block)
        raise KeyError("Key {} not found.".format(key))

    def __delitem__(self, key):
        del self._block_store[key]

    def __contains__(self, x):
        return x in self._block_store

    def __iter__(self):
        return iter(self._block_store)

    def __len__(self):
        return len(self._block_store)

    def __str__(self):
        out = []
        for v in self._block_store.values():
            out.append(str(v))
        return ','.join(out)

    def update_chain(self, new_chain, old_chain):
        """
        Set the current chain head, does not validate that the block
        store is in a valid state, ie that all the head block and all
        predecessors are in the store.

        :param new_chain: The new block of the new chain.
        :param old_chain: The block of the existing chain to remove from the
        store.
        :return:
        None
        """
        add_pairs = []
        del_keys = []
        for v in new_chain:
            add_pairs = add_pairs + self._build_add_block_ops(v)
        for v in old_chain:
            del_keys = del_keys + self._build_remove_block_ops(v)
        add_pairs.append(("chain_head_id", new_chain[0].identifier))

        self._block_store.set_batch(add_pairs, del_keys)

    @property
    def chain_head(self):
        """
        Return the head block of the current chain.
        """
        if "chain_head_id" not in self._block_store:
            return None
        return self.__getitem__(self._block_store["chain_head_id"])

    @property
    def store(self):
        """
        Access to the underlying store dict.
        """
        return self._block_store

    @staticmethod
    def wrap_block(blkw):
        return {
            "block": blkw.block,
            "weight": blkw.weight
        }

    @staticmethod
    def _build_add_block_ops( blkw):
        """Build the batch operations to add a block to the BlockStore.

        :param blkw (BlockWrapper): block to add
        :return:
        list of key value tuples to add to the BlockStore
        """
        out = []
        blk_id = blkw.identifier
        out.append((blk_id, BlockStore.wrap_block(blkw)))
        for b in blkw.batches:
            out.append((b.header_signature, blk_id))
            for t in b.transactions:
                out.append((t.header_signature, blk_id))
        return out

    @staticmethod
    def _build_remove_block_ops(self, blkw):
        """Build the batch operations to remove a block to the BlockStore.

        :param blkw (BlockWrapper): block to remove
        :return:
        list of values to remove from the BlockStore
        """
        out = []
        blk_id = blkw.identifier
        out.append(blk_id)
        for b in blkw.batches:
            out.append(b.header_signature)
            for t in b.transactions:
                out.append(t.header_signature)
        return out

    def get_block_by_transaction_id(self, txn_id):
        return self.__getitem__(self._block_store[txn_id])

    def has_transaction(self, txn_id):
        return txn_id in self._block_store

    def get_block_by_batch_id(self, batch_id):
        return batch_id in self._block_store

