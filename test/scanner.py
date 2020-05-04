#!/usr/bin/env python3
import jupyterhubutils as jhu
q = jhu.SingletonScanner(name='sciplat-lab', owner='lsstsqre', debug=True,
                         experimentals=3, cachefile="/tmp/reposcan.json")
q.scan()
q.get_all_tags()
