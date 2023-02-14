import json
import logging
from dataclasses import dataclass
from os import makedirs, path

from dumper_utils.path import get_dname_for_data, get_fname_for_method_result

from vk_api import VkApi, VkTools
from vk_api.exceptions import ApiError, VkToolsException


logger = logging.getLogger()


TOO_BIG_RESPONSE_ERR = 13


def _api_request(vk_session: VkApi, vk_tools: VkTools, method_name: str, data: dict, **kwargs) -> dict | list:
    """
    If VK API supports pagination of `method_name` method, you should use
    a `count` parameter with the value equal to the maximum page size
    (see dev.vk.com/method for a description of the `count` parameter
    for each method you use).
    """
    if "count" in kwargs:
        count = kwargs["count"]
        # Loop bypasses the API error `response size is too big`
        while True:
            try:
                return vk_tools.get_all(method_name, count, data)
            except ApiError as e:
                if e.code != TOO_BIG_RESPONSE_ERR or count == 1:
                    raise e
                count = (count + 5) // 5
                logger.info("The page size is reduced to %s for `%s` method", count, method_name)
    else:
        return vk_session.method(method_name, data)


@dataclass
class Parser:
    owner_id: int
    vk_session: VkApi
    stat_beg_timestamp: int | None

    def _enrich_wall_by_comments_and_likes(self, wall: dict, vk_tools: VkTools):

        for post in wall["items"]:

            # Likes of Post
            if post["likes"]["count"] != 0:
                req_data = {
                    "owner_id": self.owner_id,
                    "item_id": post["id"],
                    "type": "post",
                }
                vk_response = _api_request(self.vk_session, vk_tools, "likes.getList",
                                           data=req_data, count=1000)
                post["likes_info"] = vk_response

            # Comments of Post
            if post["comments"]["count"] != 0:
                req_data = {
                    "owner_id": self.owner_id,
                    "post_id": post["id"],
                    "need_likes": 1,
                }
                vk_response = _api_request(self.vk_session, vk_tools, "wall.getComments",
                                           data=req_data, count=100)
                post["comments_list"] = vk_response

            # Likes of Comments
            if post["comments"]["count"] != 0:
                for comment in post["comments_list"]["items"]:
                    if "likes" in comment and comment["likes"]["count"] != 0:
                        req_data = {
                            "owner_id": self.owner_id,
                            "item_id": comment["id"],
                            "type": "comment",
                        }
                        vk_response = _api_request(self.vk_session, vk_tools, "likes.getList",
                                                   data=req_data, count=1000)
                        comment["likes_info"] = vk_response

    def _enrich_topics_by_comments_and_likes(self, topics: dict, vk_tools: VkTools):

        for topic in topics["items"]:

            # Comments of Title
            req_data = {
                "group_id": -self.owner_id,
                "topic_id": topic["id"],
                "need_likes": 1,
            }
            vk_response = _api_request(self.vk_session, vk_tools, "board.getComments",
                                       data=req_data, count=100)
            topic["topics_info"] = vk_response

            # Likes of Comments
            for comment in topic["topics_info"]["items"]:
                if "likes" in comment and comment["likes"]["count"] != 0:
                    req_data = {
                        "owner_id": self.owner_id,
                        "item_id": comment["id"],
                        "type": "topic_comment",
                    }
                    vk_response = _api_request(self.vk_session, vk_tools, "likes.getList",
                                               data=req_data, count=1000)
                    comment["likes_info"] = vk_response

    def _enrich_titles_by_comments_and_likes(self, titles: list, vk_tools: VkTools):
        for item in titles:
            req_data = {
                "owner_id": self.owner_id,
                "page_id": item["id"],
                "need_source": 1,
                "need_html": 1,
            }
            vk_response = _api_request(self.vk_session, vk_tools, "pages.get", data=req_data)
            item["page"] = vk_response

    def _enrich_albums_by_photos(self, albums: dict, vk_tools: VkTools):
        for album in albums["items"]:
            req_data = {
                "owner_id": self.owner_id,
                "album_id": album["id"],
                "photo_sizes": 1,
            }
            vk_response = _api_request(self.vk_session, vk_tools, "photos.get", data=req_data, count=1000)
            album["photos_list"] = vk_response

    def _dump(self, data: dict | list, method_name: str):
        dir_name = get_dname_for_data(self.owner_id)
        makedirs(dir_name, exist_ok=True)
        out_fname = path.join(dir_name, get_fname_for_method_result(self.owner_id, method_name))
        with open(out_fname, "w") as fd:
            json.dump(data, fd, indent=4)
        logger.info("Response of method %s is saved in %s", method_name, out_fname)

    def fetch_content(self):
        """
        Make requests to vk.com API and save responses in local json files.
        Responses of some methods are enriched by additional requests
        to save comments, likes, etc.
        """

        vk_tools = VkTools(self.vk_session)

        api_requests: list[dict] = [
            {
                "method_name": "groups.getById",
                "data": {
                    "group_id": -self.owner_id,
                    "fields": ",".join([
                        "activity", "ban_info", "can_post", "can_see_all_posts", "city", "contacts",
                        "counters", "country", "cover", "description", "finish_date", "fixed_post",
                        "links", "market", "members_count", "place", "site", "start_date", "status",
                        "verified", "wiki_page"
                    ]),
                },
            },
            {
                "method_name": "wall.get",
                "data": {"owner_id": self.owner_id},
                "count": 100,
                "enrich_func": self._enrich_wall_by_comments_and_likes,
            },
            {
                "method_name": "board.getTopics",
                "data": {"group_id": -self.owner_id},
                "count": 100,
                "enrich_func": self._enrich_topics_by_comments_and_likes,
            },
            {
                "method_name": "video.get",
                "data": {"owner_id": self.owner_id},
                "count": 100,
            },
            {
                "method_name": "docs.get",
                "data": {"owner_id": self.owner_id},
                "count": 2000,
            },
            {
                "method_name": "groups.getMembers",
                "data": {
                    "group_id": -self.owner_id,
                    "sort": "id_asc",
                },
                "count": 1000,
            },
            {
                "method_name": "pages.getTitles",
                "data": {"group_id": -self.owner_id},
                "enrich_func": self._enrich_titles_by_comments_and_likes,
            },
            {
                "method_name": "photos.getAlbums",
                "data": {
                    "owner_id": self.owner_id,
                    "need_system": 1,
                    "need_covers": 1,
                },
                "enrich_func": self._enrich_albums_by_photos,
            },
        ]

        if self.stat_beg_timestamp:
            api_requests.append({
                "method_name": "stats.get",
                "data": {
                    "group_id": -self.owner_id,
                    "timestamp_from": self.stat_beg_timestamp,
                },
            })

        for req in api_requests:
            try:
                vk_response = _api_request(self.vk_session, vk_tools, **req)
                if "enrich_func" in req:
                    req["enrich_func"](vk_response, vk_tools)
                self._dump(vk_response, req["method_name"])
            except (ApiError, VkToolsException) as e:
                logger.warning("Failed to make request %s: %s", req["method_name"], e)
