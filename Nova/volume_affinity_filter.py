# Copyright 2024 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from cinderclient import exceptions as cinder_exc
from oslo_log import log as logging

from nova.scheduler import filters
from nova.volume import cinder


LOG = logging.getLogger(__name__)


class VolumeAffinityFilter(filters.BaseHostFilter):
    """Schedule instance on the same host as its volume.

    This filter ensures instances are scheduled on hosts where their volumes
    are located, which is particularly important for certain volume backends
    like LVM, where data locality is important for performing well.

    The filter works by:
    1. Getting the volume host from Cinder using the same_volume_host hint
    2. Comparing the volume's host with each compute host
    3. Passing only the host that matches the volume's host

    Note:
    * Only works with volume drivers where host affinity is meaningful
    * Requires the volume to exist and be accessible
    * Uses the same_volume_host from scheduler_hints

    The filter is useful for optimizing I/O performance and reducing network
    traffic between compute and storage nodes by ensuring volume and instance
    locality.
    """

    RUN_ON_REBUILD = False

    def host_passes(self, host_state, spec_obj):
        """Check if host matches volume's location."""

        volume_id = spec_obj.get_scheduler_hint('same_volume_host')
        if not volume_id:
            LOG.debug("No same_volume_host hint provided, skipping filter")
            return True

        try:
            ctxt = spec_obj._context.elevated()
            volume = cinder.cinderclient(ctxt).volumes.get(volume_id)

            volume_host = getattr(volume, 'os-vol-host-attr:host', None)
            if not volume_host:
                LOG.warning(f"Could not determine host for volume {volume_id}")
                return False

            volume_host_name = volume_host.split('@')[0]

            if host_state.host == volume_host_name:
                LOG.debug(
                    f"Host {host_state.host} matches volume location for "
                    f"volume {volume_id}"
                )
                return True

            LOG.debug(
                f"Host {host_state.host} does not match volume location "
                f"{volume_host_name} for volume {volume_id}"
            )
            return False

        except cinder_exc.NotFound:
            LOG.warning(f"Volume {volume_id} not found in Cinder")
            return False
        except Exception as ex:
            LOG.error(
                f"Error checking volume location for volume {volume_id}: {ex}")
            return False
