#!/usr/bin/env python3
import argparse
from .reaper import Reaper


def standalone():
    args = parse_args()
    wilford_grimly = Reaper(host=args.repo,
                            owner=args.owner,
                            name=args.name,
                            keep_dailies=args.dailies,
                            keep_weeklies=args.weeklies,
                            keep_experimentals=args.experimentals,
                            port=args.port,
                            cachefile=args.cachefile,
                            debug=args.debug)

    wilford_grimly.more_cowbell()


def parse_args():
    """Parse command-line arguments"""
    desc = "Remove obsolete lab images"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("-d", "--debug", action="store_true",
                        help="enable debugging")
    parser.add_argument("-r", "--repo", "--repository",
                        help="Docker repository host")
    parser.add_argument("-o", "--owner", "--organization", "--org",
                        help="repository owner [lsstsqre]",
                        default="lsstsqre")
    parser.add_argument("-n", "--name",
                        help="repository name [sciplat-lab]",
                        default="sciplat-lab")
    parser.add_argument("-q", "--dailies", "--daily", "--quotidian", type=int,
                        help="# of daily builds to keep [15]",
                        default=15)
    parser.add_argument("-w", "--weeklies", "--weekly", type=int,
                        help="# of weekly builds to keep [78]",
                        default=78)
    parser.add_argument("-e", "--experimentals", "--experimental",
                        "--exp", type=int,
                        help="# of experimental builds to keep [10]",
                        default=10)
    parser.add_argument("-p", "--port", help="Repository port [443 for" +
                        " secure, 80 for insecure]",
                        default=None)
    parser.add_argument("-f", "--cachefile", help="Cachefile for results " +
                        " [None]", default=None)
    results = parser.parse_args()
    return results


if __name__ == "__main__":
    standalone()
