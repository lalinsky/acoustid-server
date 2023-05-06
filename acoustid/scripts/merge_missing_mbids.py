#!/usr/bin/env python

# Copyright (C) 2011 Lukas Lalinsky
# Distributed under the MIT license, see the LICENSE file for details.

import logging
from acoustid.script import Script
from acoustid.data.track import merge_missing_mbids

logger = logging.getLogger(__name__)


def run_merge_missing_mbid(script: Script, mbid: str):
    if script.config.cluster.role != 'master':
        logger.info('Not running merge_missing_mbids in slave mode')
        return

    with script.context() as ctx:
        merge_missing_mbids(ctx.db.get_fingerprint_db(), ctx.db.get_ingest_db(), only_mbid=mbid)


def run_merge_missing_mbids(script, opts, args):
    if script.config.cluster.role != 'master':
        logger.info('Not running merge_missing_mbids in slave mode')
        return

    with script.context() as ctx:
        merge_missing_mbids(ctx.db.get_fingerprint_db(), ctx.db.get_ingest_db())
