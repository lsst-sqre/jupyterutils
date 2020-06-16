# specifically use concurrent.futures for threadsafety
# asyncio Futures cannot be used across threads
import time
from kubernetes import watch
from kubespawner.reflector import NamespacedResourceReflector
# This is kubernetes client implementation specific, but we need to know
# whether it was a network or watch timeout.
from urllib3.exceptions import ReadTimeoutError


class LSSTResourceReflector(NamespacedResourceReflector):
    """
    Just like the superclass but turning off a bunch of noisy and unhelpful
    logging messages.
    """

    def _watch_and_update(self):
        """
        Keeps the current list of resources up-to-date

        This method is to be run not on the main thread!

        We first fetch the list of current resources, and store that. Then we
        register to be notified of changes to those resources, and keep our
        local store up-to-date based on these notifications.

        We also perform exponential backoff, giving up after we hit 32s
        wait time. This should protect against network connections dropping
        and intermittent unavailability of the api-server. Every time we
        recover from an exception we also do a full fetch, to pick up
        changes that might've been missed in the time we were not doing
        a watch.

        Note that we're playing a bit with fire here, by updating a dictionary
        in this thread while it is probably being read in another thread
        without using locks! However, dictionary access itself is atomic,
        and as long as we don't try to mutate them (do a 'fetch / modify /
        update' cycle on them), we should be ok!
        """
        selectors = []
        if self.label_selector:
            selectors.append("label selector=%r" % self.label_selector)
        if self.field_selector:
            selectors.append("field selector=%r" % self.field_selector)

        cur_delay = 0.1

        while True:
            start = time.monotonic()
            w = watch.Watch()
            try:
                resource_version = self._list_and_update()
                if not self.first_load_future.done():
                    # signal that we've loaded our initial data
                    self.first_load_future.set_result(None)
                watch_args = {
                    'namespace': self.namespace,
                    'label_selector': self.label_selector,
                    'field_selector': self.field_selector,
                    'resource_version': resource_version,
                }
                if self.request_timeout:
                    # set network receive timeout
                    watch_args['_request_timeout'] = self.request_timeout
                if self.timeout_seconds:
                    # set watch timeout
                    watch_args['timeout_seconds'] = self.timeout_seconds
                # in case of timeout_seconds, the w.stream just exits (no exception thrown)
                # -> we stop the watcher and start a new one
                for watch_event in w.stream(
                    getattr(self.api, self.list_method_name),
                    **watch_args
                ):
                    # Remember that these events are k8s api related WatchEvents
                    # objects, not k8s Event or Pod representations, they will
                    # reside in the WatchEvent's object field depending on what
                    # kind of resource is watched.
                    #
                    # ref: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.16/#watchevent-v1-meta
                    # ref: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.16/#event-v1-core
                    cur_delay = 0.1
                    resource = watch_event['object']
                    if watch_event['type'] == 'DELETED':
                        # This is an atomic delete operation on the dictionary!
                        self.resources.pop(resource.metadata.name, None)
                    else:
                        # This is an atomic operation on the dictionary!
                        self.resources[resource.metadata.name] = resource
                    if self._stop_event.is_set():
                        break
                    watch_duration = time.monotonic() - start
                    if watch_duration >= self.restart_seconds:
                        break
            except ReadTimeoutError:
                # network read time out, just continue and restart the watch
                # this could be due to a network problem or just low activity
                continue
            except Exception:
                cur_delay = cur_delay * 2
                if cur_delay > 30:
                    self.log.exception(
                        "Watching resources never recovered, giving up")
                    if self.on_failure:
                        self.on_failure()
                    return
                self.log.exception(
                    "Error watching resources, retrying in %ss", cur_delay)
                time.sleep(cur_delay)
                continue
            else:
                pass
            finally:
                w.stop()
                if self._stop_event.is_set():
                    break
