# Copyright (C) 2015 The bitcoin-blockchain-parser developers
#
# This file is part of bitcoin-blockchain-parser.
#
# It is subject to the license terms in the LICENSE file found in the top-level
# directory of this distribution.
#
# No part of bitcoin-blockchain-parser, including this file, may be copied,
# modified, propagated, or distributed except according to the terms contained
# in the LICENSE file.
import line_profiler
import os
import mmap
import struct
import logging

from .block import Block


# Constant separating blocks in the .blk files
BITCOIN_CONSTANT = b"\xf9\xbe\xb4\xd9"

def get_files(path, startingblock = 0):
    """
    Given the path to the .bitcoin directory, returns the sorted list of .blk
    files contained in that directory
    to shorten search times a startingblock can be entered, all unnecessary blk files will be skipped
    basis is blk.txt files with the first block height for each file
    as the blocks can be unordered, and the files are different on each node, its recommended to make a new file on a every node
    for safety one extra blk.dat will be loaded, and the last 2 blk will be ignored
    """
    d = {}
    latestblk =0
    blknum = 0
    with open("blk.txt") as f:
        for line in f:
            (key, val) = line.split(sep= ' : ')
            d[int(key)] = int(val)
            if int(key) >= latestblk and int(key) >= 1:
                latestblk = int(key) - 1
    blknum = 0
    for i in range(0,latestblk):
        if startingblock >= d[i]:
            blknum = i
    if blknum >=2:
        blknum -= 2
    files = os.listdir(path)
    files = [f for f in files if f.startswith("blk") and f.endswith(".dat") and int(f[3:8]) >= blknum and int(f[3:8]) < latestblk]
    files = map(lambda x: os.path.join(path, x), files)
    return sorted(files)

def get_blocks(blockfile):
    """
    Given the name of a .blk file, for every block contained in the file,
    yields its raw hexadecimal value
    """
    with open(blockfile, "rb") as f:
        # Unix-only call, will not work on Windows, see python doc.
        raw_data = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        length = len(raw_data)
        offset = 0
        block_count = 0
        while offset < (length - 4):
            if raw_data[offset:offset+4] == BITCOIN_CONSTANT:
                offset += 4
                size = struct.unpack("<I", raw_data[offset:offset+4])[0]
                offset += 4 + size
                block_count += 1
                yield raw_data[offset-size:offset]
            else:
                offset += 1
        raw_data.close()

class Blockchain(object):
    """Represent the blockchain contained in the series of .blk files
    maintained by bitcoind.
    """

    def __init__(self, path):
        self.path = path

    def get_unordered_blocks(self, startingblk = 0):
        """Yields the blocks contained in the .blk files as is,
        without ordering them according to height.
        """
        for blk_file in get_files(self.path, startingblk):
            for raw_block in get_blocks(blk_file):
                yield Block(raw_block)
    
    def get_main_chain(self, startingblock = "0" * 64, startingblock_height = 0):
        """Yields the blocks contained in the main chain in the right order,
        from the startingblock to the chain tip. Each returned block has its
        height attribute set correctly. Blocks that aren't present in
        the mainchain aren't returned by this method, use get_unordered_blocks
        to get *all* blocks stored in the .blk files
        """

        genesis = startingblock
        print('startingblock is ' + startingblock)
        tip = None
        predecessor = {}

        # Linking blocks to their precedessor
        # We suppose the best chain tip is the last block written by bitcoind,
        # it is not very robust but is enough for simple uses
        for block in self.get_unordered_blocks(startingblock_height):
            predecessor[block.hash] = block.header.previous_block_hash
            tip = block.hash

        logging.debug("All blocks are read")
        print('all blocks are read')
        # Building the mainchain by going from the tip to the genesis block
        mainchain = []
        while tip != genesis:
            mainchain.append(tip)
            tip = predecessor[tip]

        logging.debug("Mainchain constructed")
        # Having built the mainchain, we can begin to return full blocks in the
        # right order
        prev_hash = genesis
        # Holding out-of-order blocks in a dict, keys are hashes
        blocks = {}
        height = startingblock_height
        # This is the block to fetch
        print('Mainchain constructed')
        next_hash = mainchain.pop()
        for block in self.get_unordered_blocks(startingblock_height):
            if len(mainchain) == 0:
                print('mainchain is 0')
                raise StopIteration()

            # If it has already been seen, we return it
            if next_hash in blocks:
                seen = blocks[next_hash]
                assert(seen.header.previous_block_hash == prev_hash)
                seen.height = height
                height += 1
                yield seen
                prev_hash = next_hash
                del blocks[next_hash]
                next_hash = mainchain.pop()
            # Else, if it's the current block, we return it
            if block.hash == next_hash:
                assert(block.header.previous_block_hash == prev_hash)
                block.height = height
                height += 1
                yield block
                prev_hash = next_hash
                next_hash = mainchain.pop()
            # Else, we store the current block and go to the next one
            else:
                blocks[block.hash] = block

