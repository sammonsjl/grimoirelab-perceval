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
from grimoirelab_toolkit.uris import urijoin

from perceval.client import HttpClient

CATEGORY_MESSAGE = "message"
MAX_ITEMS = 100
TEST_FROM_DATE = "2021-06-01T13:02:10Z"

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
                emailAddress
              }
            }
            image
            id
            name
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
                        emailAddress
                      }
                    }
                    image
                    id
                    name
                  }
                  dateCreated
                  friendlyUrlPath
                  id
                }
              }
              creator {
                userAccount: graphQLNode {
                  ... on UserAccount {
                    emailAddress
                  }
                }
                image
                id
                name
              }
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


class LiferayClient(HttpClient):
    """Liferay API client.

    This class implements a simple client to retrieve entities from
    any Liferay system.
    """

    def __init__(self, url, site_id,
                 max_items=MAX_ITEMS,
                 archive=None, from_archive=False):
        super().__init__(url, archive=archive, from_archive=from_archive)

        self.url = url
        self.site_id = site_id
        self.max_items = max_items
        self.graphql_url = urijoin(url, 'o', 'graphql')

    def fetch_items(self, query_template, from_date=None):
        """Retrieve all the items from a given Liferay Site.

        :param query_template: GraphQL query to use to retrieve data.
        :param from_date: obtain posts updated since this date
        """
        page = 1

        query = query_template % (from_date, page, self.max_items, self.site_id)
        response = self.fetch(self.graphql_url, payload=json.dumps({'query': query}), method=HttpClient.POST)
        items = response.json()

        tquestions = items['data']['entries']['totalCount']
        nquestions = items['data']['entries']['pageSize']

        self.__log_status(page, tquestions, self.url)

        has_next = True
        while has_next:
            yield items['data']['entries']['items']
            page += 1

            query = query_template % (from_date.isoformat(), page, self.max_items, self.site_id)
            response = self.fetch(self.graphql_url, payload=json.dumps({'query': query}), method=HttpClient.POST)
            items = response.json()

            nquestions += items['data']['entries']['pageSize']

            if page == items['data']['entries']['lastPage']:
                has_next = False

            self.__log_status(page, tquestions, self.url)

    def posts(self, from_date=None):
        """
        Retrieve all message board messages from Liferay Site
        """

        return self.fetch_items(POSTS_QUERY_TEMPLATE, TEST_FROM_DATE)

    @staticmethod
    def __log_status(max_items, total, url):
        if total != 0:
            nitems = min(max_items, total)
            logger.info("Fetching %s/%s items from %s" % (nitems, total, url))
        else:
            logger.info("No items were found for %s" % url)


def main():
    client = LiferayClient("http://localhost:8080", 14)
    posts = client.posts()

    for post in posts:
        print(post['headline'])


main()
