from jupyterhub.handlers import LogoutHandler
import os


class LSSTLogoutHandler(LogoutHandler):
    '''Redirect to (JWT OAuth proxy) OAuth2 sign_in, which also logs you
    out.  I dunno, man.  Blame BVan.
    '''

    async def render_logout_page(self):
        logout_url = os.getenv("LOGOUT_URL") or "/oauth2/sign_in"
        self.redirect(logout_url, permanent=False)
