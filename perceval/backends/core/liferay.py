# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-present Bitergia
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
#     Jamie Sammons <jamie.sammons@liferay.com>
#
import json
import logging
import urllib3

from grimoirelab_toolkit.uris import urijoin
from grimoirelab_toolkit.datetime import (datetime_to_utc,
                                          str_to_datetime)
from urllib3.exceptions import InsecureRequestWarning

from perceval.client import HttpClient
from ...backend import (Backend,
                        BackendCommand,
                        BackendCommandArgumentParser)

from ...utils import DEFAULT_DATETIME

CATEGORY_QUESTION = "question"
MAX_ITEMS = 20

POSTS_QUERY_TEMPLATE = """
    {
      entries: messageBoardThreads(
            filter: "dateCreated gt %s",
            flatten: true,
            page: %s,
            pageSize: %s,
            siteKey: "%s",
            sort: "dateCreated:desc"
        )
        {
        items {
          aggregateRating {
            ratingAverage
            ratingCount
            ratingValue
          }
          articleBody
          creator {
            userAccount: graphQLNode {
              ... on UserAccount {
                alternateName
                emailAddress
              }
            }
            image
            id
            name
            profileURL
          }
          creatorStatistics {
            joinDate
            lastPostDate
            postsNumber
          }
          dateCreated
          dateModified
          friendlyUrlPath
          headline
          id
          hasValidAnswer
          keywords
          taxonomyCategoryBriefs {
            taxonomyCategoryName
          }
          id
          viewCount
          answers: messageBoardMessages {
            items {
              aggregateRating {
                ratingAverage
                ratingCount
                ratingValue
              }
              articleBody
              comments: messageBoardMessages(flatten: true) {
                items {
                  creator {
                    userAccount: graphQLNode {
                      ... on UserAccount {
                        alternateName
                        emailAddress
                      }
                    }
                    image
                    id
                    name
                    profileURL
                  }
                  dateCreated
                  friendlyUrlPath
                  headline
                  id
                }
              }
              creator {
                userAccount: graphQLNode {
                  ... on UserAccount {
                    alternateName
                    emailAddress
                  }
                }
                image
                id
                name
                profileURL
              }
              dateCreated
              headline
              numberOfMessageBoardMessages
              id
              showAsAnswer
            }
          }
          dateCreated
          dateModified
          friendlyUrlPath
          headline
          numberOfMessageBoardMessages
        }
        lastPage
        page
        pageSize
        totalCount
          }
        }
    """

logger = logging.getLogger(__name__)


class Liferay(Backend):
    """Liferay backend for Perceval.

    This class retrieves blog entries and forum messages stored
    in Liferay system.To initialize this class the URL must be provided.
    The `url` will be set as the origin of the data.

    :param url: URL of the Liferay server
    :param group_id: Liferay Site to fetch data from
    :param user: Liferay's username
    :param password: Liferay's password
    :param verify: allows to disable SSL verification
    :param cert: SSL certificate
    :param max_results: max number of results per query
    :param tag: label used to mark the data
    :param archive: archive to store/retrieve items
    """
    version = '0.5.0'

    CATEGORIES = [CATEGORY_QUESTION]

    def __init__(self, url, group_id,
                 user=None, password=None,
                 verify=True, cert=None,
                 max_results=MAX_ITEMS, tag=None,
                 archive=None):
        origin = url

        super().__init__(origin, tag=tag, archive=archive)
        self.url = url
        self.group_id = group_id
        self.user = user
        self.password = password
        self.verify = verify
        self.cert = cert
        self.max_results = max_results
        self.client = None

    def fetch(self, category=CATEGORY_QUESTION, from_date=DEFAULT_DATETIME, filter_classified=False):
        """Fetch the entries from the site.

        The method retrieves, from a Liferay site

        :param category: the category of items to fetch
        :param from_date: obtain issues/pull requests updated since this date
        :param filter_classified: remove classified fields from the resulting items

        :returns: a generator of issues
        """

        if not from_date:
            from_date = DEFAULT_DATETIME

        from_date = datetime_to_utc(from_date)

        kwargs = {'from_date': from_date}
        items = super().fetch(category, **kwargs)

        return items

    def fetch_items(self, category, **kwargs):
        """Fetch the questions

        :param category: the category of items to fetch
        :param kwargs: backend arguments

        :returns: a generator of items
        """
        from_date = kwargs['from_date']

        logger.info("Looking for questions at site '%s', with tag '%s' and updated from '%s'",
                    self.url, self.tag, str(from_date))

        whole_pages = self.client.get_questions(from_date)

        for whole_page in whole_pages:
            questions = self.parse_questions(whole_page)
            for question in questions:
                yield question

    @classmethod
    def has_archiving(cls):
        """Returns whether it supports archiving items on the fetch process.

        :returns: this backend supports items archive
        """
        return True

    @classmethod
    def has_resuming(cls):
        """Returns whether it supports to resume the fetch process.

        :returns: this backend supports items resuming
        """
        return False

    @staticmethod
    def metadata_id(item):
        """Extracts the identifier from a Liferay item."""

        return str(item['id'])

    @staticmethod
    def metadata_updated_on(item):
        """Extracts the update time from a Liferay item.

        The timestamp is extracted from 'modifiedDate' field.
        This date is a UNIX timestamp but needs to be converted to
        a float value.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        ts = item['dateModified']
        ts = str_to_datetime(ts)

        return ts.timestamp()

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a Liferay item.

        This backend generates three types of item which are
        'blog', 'message' and 'user'.
        """

        return CATEGORY_QUESTION

    @staticmethod
    def parse_questions(raw_page):
        """Parse a StackExchange API raw response.

        The method parses the API response retrieving the
        questions from the received items

        :param raw_page: items from where to parse the questions

        :returns: a generator of questions
        """
        raw_questions = json.loads(raw_page)
        questions = raw_questions['data']['entries']['items']
        for question in questions:
            yield question

    def _init_client(self, from_archive=False):
        """Init client"""

        return LiferayClient(self.url, self.group_id, self.user, self.password,
                             self.verify, self.cert, self.max_results,
                             self.archive, from_archive)


class LiferayClient(HttpClient):
    """Liferay API client.

    This class implements a simple client to retrieve entities from
    any Liferay system.
    """

    def __init__(self, url, site_id, user=None, password=None,
                 verify=None, cert=None,
                 max_items=MAX_ITEMS,
                 archive=None, from_archive=False):
        super().__init__(url, archive=archive, from_archive=from_archive)

        self.url = url
        self.site_id = site_id
        self.max_items = max_items
        self.graphql_url = urijoin(url, 'o', 'graphql')
        self.user = user
        self.password = password
        self.cert = cert
        self.verify = verify

        if not from_archive:
            self.__init_session()

    def fetch_items(self, query_template, from_date=None):
        """Retrieve all the items from a given Liferay Site.

        :param query_template: GraphQL query to use to retrieve data.
        :param from_date: obtain posts updated since this date
        """
        page = 1

        query = query_template % (from_date.isoformat(), page, self.max_items, self.site_id)
        response = self.fetch(self.graphql_url, payload=json.dumps({'query': query}), method=HttpClient.POST)
        items = response.text
        data = response.json()

        tquestions = data['data']['entries']['totalCount']
        nquestions = data['data']['entries']['pageSize']

        self.__log_status(nquestions, tquestions, self.url)

        has_next = True
        while has_next:
            yield items
            page += 1

            query = query_template % (from_date.isoformat(), page, self.max_items, self.site_id)
            response = self.fetch(self.graphql_url, payload=json.dumps({'query': query}), method=HttpClient.POST)
            items = response.text
            data = response.json()

            nquestions += data['data']['entries']['pageSize']

            if page >= data['data']['entries']['lastPage']:
                has_next = False

            self.__log_status(nquestions, tquestions, self.url)

    def get_questions(self, from_date=None):
        """
        Retrieve all message board messages from Liferay Site
        """

        return self.fetch_items(POSTS_QUERY_TEMPLATE, from_date)

    def __init_session(self):
        if (self.user and self.password) is not None:
            self.session.auth = (self.user, self.password)

        if self.cert:
            self.session.cert = self.cert

        if self.verify is not True:
            urllib3.disable_warnings(InsecureRequestWarning)
            self.session.verify = False

    @staticmethod
    def __log_status(max_items, total, url):
        if total != 0:
            nitems = min(max_items, total)
            logger.info("Fetching %s/%s items from %s" % (nitems, total, url))
        else:
            logger.info("No items were found for %s" % url)


class LiferayCommand(BackendCommand):
    """Class to run Liferay backend from the command line."""

    BACKEND = Liferay

    @classmethod
    def setup_cmd_parser(cls):
        """Returns the Liferay argument parser."""

        parser = BackendCommandArgumentParser(cls.BACKEND,
                                              basic_auth=True,
                                              from_date=True,
                                              archive=True)

        # Liferay options
        group = parser.parser.add_argument_group('Liferay arguments')
        group.add_argument('--site-id', dest='group_id',
                           help="Site to fetch data from")
        group.add_argument('--verify', default=True,
                           help="Value 'False' disables SSL verification")
        group.add_argument('--cert',
                           help="SSL certificate path (PEM)")
        group.add_argument('--max-results', dest='max_results',
                           type=int, default=MAX_ITEMS,
                           help="Maximum number of results requested in the same query")

        # Required arguments
        parser.parser.add_argument('url',
                                   help="Liferay's url")

        return parser
