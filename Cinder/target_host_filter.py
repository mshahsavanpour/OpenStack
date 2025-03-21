# -*- coding: utf-8 -*-
# Copyright 2014, Adrien Vergé <adrien.verge@numergy.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from oslo_log import log as logging

from cinder.i18n import _
from cinder.scheduler import filters
from cinder.volume import volume_utils


LOG = logging.getLogger(__name__)

HINT_KEYWORD = 'target_host'


class TargetHostFilter(filters.BaseBackendFilter):
    """Filter to schedule volume creation on a specified target host.

    This filter allows the Cinder scheduler to provision volumes on a
    particular storage backend, as identified by the 'target_host' scheduler
    hint.

    To utilize this filter:

    1. Include the 'target_host' key in the scheduler hints when creating a
       volume, specifying the desired host for volume placement.

    2. Ensure that the 'TargetHostFilter' is added to the list of enabled
       filters in the Cinder scheduler configuration (`cinder.conf`):

       [DEFAULT]
       scheduler_default_filters = TargetHostFilter, <other_filters>

    Without the 'target_host' hint, this filter allows all backends to pass through,
    with fallback to other active filters for scheduling.
    """

    def backend_passes(self, backend_state, filter_properties):
        scheduler_hints = filter_properties.get('scheduler_hints') or {}
        target_host = scheduler_hints.get(HINT_KEYWORD, None)

        if not target_host:
            return True

        backend_host = volume_utils.extract_host(backend_state.backend_id, 'host')

        matches = backend_host == target_host
        if not matches:
            LOG.debug(f"Backend {backend_host} filtered out: does not match target "
                     f"host {target_host}")

        return matches
