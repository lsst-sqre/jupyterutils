#!/usr/bin/env python3
import jupyterhubutils as jhu
q = jhu.SingletonScanner(
    host='ts-dockerhub.lsst.org',
    name='sciplat-lab',
    owner='test',
    username='lsst_jenkins',
    password='',
    debug=True,
    experimentals=3,
    cachefile="/tmp/reposcan.json")
q.scan()
q.get_all_tags()
