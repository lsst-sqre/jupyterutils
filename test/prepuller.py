#!/usr/bin/env python3
import jupyterhubutils as jhu
lc = jhu.LSSTConfig()
args = jhu.scanrepo.parse_args(cfg=lc, component="prepuller")
q = jhu.Prepuller(args=args)
q.update_images_from_repo()
