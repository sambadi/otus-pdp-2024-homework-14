#!/usr/bin/env python
# -*- coding: utf-8 -*-
import glob
import os
import gzip
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from optparse import OptionParser

import appsinstalled_pb2
import collections
import memcache

NORMAL_ERR_RATE = 0.01

AppsInstalled = collections.namedtuple(
    "AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"]
)
ParsingResult = collections.namedtuple(
    "ParsingResult", ["file_path", "processed", "errors", "error_rate"]
)

SupportedExecutorsMap = {"thread": ThreadPoolExecutor, "process": ProcessPoolExecutor}


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return None
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return None
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isdigit()]
    except ValueError:
        logging.info(f"Not all user apps are digits: `{line}`")
        apps = []
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info(f"Invalid geo coords: `{line}`")
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def insert_appsinstalled(memc_client: memcache.Client, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = f"{appsinstalled.dev_type}:{appsinstalled.dev_id}"
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    try:
        if dry_run:
            logging.debug(
                f"{memc_client.servers[0]} - {key} -> {str(ua).replace("\n", " ")}"
            )
        else:
            memc_client.set(key, packed)
    except Exception as e:
        logging.exception(f"Cannot write to memc: {e}")
        return False
    return True


def process_file(device_memc, file_path, log_file, dry_run=False):
    # To restore logging configuration under ProcessPoolExecutor
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO if not dry_run else logging.DEBUG,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )

    processed = errors = 0
    memc_clients = {
        memc_type: memcache.Client([memc_addr])
        for memc_type, memc_addr in device_memc.items()
    }

    with gzip.open(file_path, "rt") as fd:
        for line in fd:
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_client = memc_clients.get(appsinstalled.dev_type)
            if not memc_client:
                errors += 1
                logging.error(f"Unknow device type: {appsinstalled.dev_type}")
                continue
            ok = insert_appsinstalled(memc_client, appsinstalled, dry_run)
            if ok:
                processed += 1
            else:
                errors += 1

    dot_rename(file_path)

    err_rate = float(errors) / processed if processed else 0

    for memc_client in memc_clients.values():
        if memc_client.quit_all():
            memc_client.disconnect_all()

    return ParsingResult(
        file_path=file_path, processed=processed, errors=errors, error_rate=err_rate
    )


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    executor = SupportedExecutorsMap.get(options.executor)
    if not executor:
        logging.error(f"Executor of type {options.executor} isn`t supported!")

    with executor(max_workers=5) as executor:
        futures = [
            executor.submit(process_file, device_memc, fn, options.log, options.dry)
            for fn in sorted(glob.iglob(options.pattern))
        ]

        for future in as_completed(futures):
            result: ParsingResult = future.result()
            logging.info(f"File parsed {result.file_path}")
            if result.error_rate < NORMAL_ERR_RATE:
                logging.info(
                    f"Acceptable error rate ({result.error_rate}). Successful load"
                )
            else:
                logging.error(
                    f"High error rate ({result.error_rate} > {NORMAL_ERR_RATE}). Failed load"
                )


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("--executor", action="store", default="thread")
    (opts, args) = op.parse_args()
    logging.basicConfig(
        filename=opts.log,
        level=logging.INFO if not opts.dry else logging.DEBUG,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )

    if opts.test:
        prototest()
        sys.exit(0)

    logging.info(f"Memc loader started with options: {opts}")
    try:
        main(opts)
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
        sys.exit(1)
