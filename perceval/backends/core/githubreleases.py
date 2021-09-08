# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2020 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Jamie L Sammons <jamie.sammons@liferay.com>
#

import json
import logging

from grimoirelab_toolkit.datetime import (datetime_to_utc,
                                          str_to_datetime)


from perceval.backends.core.github import (GitHub,
                                           GitHubClient,
                                           GitHubCommand,
                                           DEFAULT_SLEEP_TIME,
                                           MIN_RATE_LIMIT,
                                           MAX_RETRIES,
                                           MAX_CATEGORY_ITEMS_PER_PAGE)

from ...utils import DEFAULT_DATETIME, DEFAULT_LAST_DATETIME

CATEGORY_RELEASE = "release"

GITHUB_API_URL = "https://api.github.com"

logger = logging.getLogger(__name__)


class GitHubRelease(GitHub):
    """GitHub backend for Perceval.

    This class allows the fetch the issues stored in GitHub
    repository. Note that since version 0.20.0, the `api_token` accepts
    a list of tokens, thus the backend must be initialized as follows:
    ```
    GitHub(
        owner='chaoss', repository='grimoirelab',
        api_token=[TOKEN-1, TOKEN-2, ...], sleep_for_rate=True,
        sleep_time=300
    )
    ```

    :param owner: GitHub owner
    :param repository: GitHub repository from the owner
    :param api_token: list of GitHub auth tokens to access the API
    :param github_app_id: GitHub App ID
    :param github_app_pk_filepath: GitHub App private key PEM file path
    :param base_url: GitHub URL in enterprise edition case;
        when no value is set the backend will be fetch the data
        from the GitHub public site.
    :param tag: label used to mark the data
    :param archive: archive to store/retrieve items
    :param sleep_for_rate: sleep until rate limit is reset
    :param min_rate_to_sleep: minimum rate needed to sleep until
         it will be reset
    :param max_retries: number of max retries to a data source
        before raising a RetryError exception
    :param max_items: max number of category items (e.g., issues,
        pull requests) per query
    :param sleep_time: time to sleep in case
        of connection problems
    :param ssl_verify: enable/disable SSL verification
    """
    version = '0.3.0'

    CATEGORIES = [CATEGORY_RELEASE]

    def __init__(self, owner=None, repository=None,
                 api_token=None, github_app_id=None, github_app_pk_filepath=None,
                 base_url=None, tag=None, archive=None,
                 sleep_for_rate=False, min_rate_to_sleep=MIN_RATE_LIMIT,
                 max_retries=MAX_RETRIES, sleep_time=DEFAULT_SLEEP_TIME,
                 max_items=MAX_CATEGORY_ITEMS_PER_PAGE, ssl_verify=True):
        super().__init__(owner, repository, api_token, github_app_id,
                         github_app_pk_filepath, base_url, tag, archive,
                         sleep_for_rate, min_rate_to_sleep, max_retries,
                         sleep_time, max_items, ssl_verify)

    def fetch(self, category=CATEGORY_RELEASE, from_date=DEFAULT_DATETIME, to_date=DEFAULT_LAST_DATETIME,
              filter_classified=False):
        """Fetch the issue events from the repository.

        The method retrieves, from a GitHub repository, the issue events
        since/until a given date.

        :param category: the category of items to fetch
        :param from_date: obtain issue events since this date
        :param to_date: obtain issue events until this date (included)
        :param filter_classified: remove classified fields from the resulting items

        :returns: a generator of events
        """
        if not from_date:
            from_date = DEFAULT_DATETIME
        if not to_date:
            to_date = DEFAULT_LAST_DATETIME

        from_date = datetime_to_utc(from_date)
        to_date = datetime_to_utc(to_date)

        kwargs = {
            'from_date': from_date,
            'to_date': to_date
        }
        items = super().fetch(category, **kwargs)

        return items

    def fetch_items(self, category, **kwargs):
        """Fetch the items

        :param category: the category of items to fetch
        :param kwargs: backend arguments

        :returns: a generator of items
        """
        from_date = kwargs['from_date']
        to_date = kwargs['to_date']

        items = self.__fetch_releases(from_date, to_date)

        return items

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a GitHub item.

        This backend generates one type item which is
        'event'.
        """
        return CATEGORY_RELEASE

    def _init_client(self, from_archive=False):
        """Init client"""

        return GitHubReleaseClient(self.owner, self.repository, self.api_token,
                                   self.github_app_id, self.github_app_pk_filepath, self.base_url,
                                   self.sleep_for_rate, self.min_rate_to_sleep,
                                   self.sleep_time, self.max_retries, self.max_items,
                                   self.archive, from_archive, self.ssl_verify)

    def __fetch_releases(self, from_date, to_date):
        """Fetch the events declared at EVENT_TYPES for issues (including pull requests)"""

        issues_groups = self.client.issues()

        for raw_issues in issues_groups:
            issues = json.loads(raw_issues)
            for issue in issues:
                issue_number = issue['number']

                is_pull = 'pull_request' in issue
                events_groups = self.client.events(issue_number, is_pull, from_date)
                for events in events_groups:
                    for event in events:

                        if str_to_datetime(event['createdAt']) > to_date:
                            return

                        event['issue'] = issue
                        yield event


class GitHubReleaseClient(GitHubClient):
    """Client for retrieving information from GitHub API

    :param owner: GitHub owner
    :param repository: GitHub repository from the owner
    :param tokens: list of GitHub auth tokens to access the API
    :param github_app_id: GitHub App ID
    :param github_app_pk_filepath: GitHub App private key PEM file path
    :param base_url: GitHub URL in enterprise edition case;
        when no value is set the backend will be fetch the data
        from the GitHub public site.
    :param sleep_for_rate: sleep until rate limit is reset
    :param min_rate_to_sleep: minimum rate needed to sleep until
         it will be reset
    :param sleep_time: time to sleep in case
        of connection problems
    :param max_retries: number of max retries to a data source
        before raising a RetryError exception
    :param max_items: max number of category items (e.g., issues,
        pull requests) per query
    :param archive: collect events already retrieved from an archive
    :param from_archive: it tells whether to write/read the archive
    :param ssl_verify: enable/disable SSL verification
    """

    def __init__(self, owner, repository, tokens=None, github_app_id=None, github_app_pk_filepath=None,
                 base_url=None, sleep_for_rate=False, min_rate_to_sleep=MIN_RATE_LIMIT,
                 sleep_time=DEFAULT_SLEEP_TIME, max_retries=MAX_RETRIES,
                 max_items=MAX_CATEGORY_ITEMS_PER_PAGE, archive=None, from_archive=False, ssl_verify=True):
        super().__init__(owner, repository, tokens, github_app_id, github_app_pk_filepath, base_url, sleep_for_rate,
                         min_rate_to_sleep, sleep_time, max_retries, max_items, archive, from_archive, ssl_verify)


class GitHubReleaseCommand(GitHubCommand):
    """Class to run GitHubQL backend from the command line."""

    BACKEND = GitHubRelease
