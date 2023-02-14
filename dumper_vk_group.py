#! /usr/bin/env python3

"""
Dump main data from a group on vk.com
"""

import datetime
import logging
import sys
from argparse import ArgumentParser
from getpass import getpass

from dumper_utils.parser import Parser
from dumper_utils.collector import FilesCollector

import vk_api
from vk_api import VkApi
from vk_api.exceptions import Captcha


logger = logging.getLogger()


def init_logger():
    sh = logging.StreamHandler()

    formatter = logging.Formatter("[%(asctime)s][%(name)+8s][%(levelname)+8s]  %(message)s")
    sh.setFormatter(formatter)

    logger.addHandler(sh)
    logger.setLevel(logging.INFO)


def vk_connect(username: str, password: str) -> VkApi:

    def captcha_handler(captcha: Captcha):
        key = input("Enter captcha code from image {}: ".format(captcha.get_url())).strip()
        return captcha.try_again(key)

    vk_session = vk_api.VkApi(login=username, password=password, captcha_handler=captcha_handler)
    try:
        vk_session.auth()
    except vk_api.AuthError as error_msg:
        sys.exit(error_msg)

    return vk_session


def resolve_group_id(string_id: str, vk_session) -> int:
    try:
        numeric_id = int(string_id)
    except ValueError as e:
        vk_response = vk_session.method("utils.resolveScreenName", {"screen_name": string_id})
        numeric_id = vk_response["object_id"]
    return -abs(numeric_id)


def main():

    init_logger()

    arg_parser = ArgumentParser()

    arg_parser.add_argument("--username", "-u", help="vk.com username")
    arg_parser.add_argument("--password", "-p", help="vk.com password")
    arg_parser.add_argument("--owner_id", "-o", help="id of group (negative number)", type=str, required=True)
    arg_parser.add_argument("--stat_beg", "-s",
                            help="date (in format DD/MM/YYYY) since which the statistics will be"
                                 " downloaded (if missing, the statistics won't be downloaded)",
                            type=str, default=None)
    arg_parser.add_argument("--verbose", "-v", help="enable verbose mode", action="store_true")

    args = arg_parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.info("Verbose mode is enabled")

    if args.stat_beg:
        try:
            dt = datetime.datetime.strptime(args.stat_beg, "%d/%m/%Y")
            stat_beg_timestamp = int(datetime.datetime.timestamp(dt))
        except Exception:
            logger.info("Failed to read the start date option of the statistics. "
                        "Use fomat `--stat_beg DD/MM/YYYY`.")
            return
    else:
        stat_beg_timestamp = None

    start_time = datetime.datetime.now()

    try:
        username = args.username or input("Username: ")
        password = args.password or getpass("Password (hidden): ")

        if not username or not password:
            logger.info("Not enough auth data")
            return

        vk_session = vk_connect(username=username, password=password)

        owner_id = resolve_group_id(args.owner_id, vk_session)

        Parser(owner_id, vk_session, stat_beg_timestamp).fetch_content()

        files_collector = FilesCollector(owner_id)
        files_collector.download_banner()
        files_collector.download_attachments()
        files_collector.download_photos()
        files_collector.download_docs()

    except KeyboardInterrupt:
        logger.info("Stopped by keyboard")
        sys.exit(0)

    finally:
        logger.info("Done in %s", (datetime.datetime.now() - start_time))


if __name__ == "__main__":
    main()
