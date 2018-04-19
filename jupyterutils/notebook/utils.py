"""Utility functions for LSST Jupyter notebook environment
"""
import bokeh
import os
import urllib


def show_with_bokeh_server(obj):
    """Method to wrap bokeh with proxy URL
    """
    def jupyter_proxy_url(port):
        """Construct proxy URL from environment
        """
        # If port is None we're asking about the URL
        # for the origin header.
        if port is None:
            return '*'

        base_url = os.environ['EXTERNAL_URL']
        service_url_path = os.environ['JUPYTERHUB_SERVICE_PREFIX']
        proxy_url_path = 'proxy/%d' % port

        user_url = urllib.parse.urljoin(base_url, service_url_path)
        full_url = urllib.parse.urljoin(user_url, proxy_url_path)
        return full_url

    bokeh.io.show(obj, notebook_url=jupyter_proxy_url)
