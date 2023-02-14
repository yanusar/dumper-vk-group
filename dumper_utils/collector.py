import aiohttp
import asyncio
import json
import logging
import requests
from collections import namedtuple
from dataclasses import dataclass, field
from os import makedirs, path

from dumper_utils.path import get_dname_for_data, get_fname_for_method_result

import aiofiles


logger = logging.getLogger()


class UnreachableCode(Exception):
    pass


IMAGE_TYPES = {
    "w": 0,
    "z": 1,
    "y": 2,
    "x": 3,
    "r": 4,
    "q": 5,
    "p": 6,
    "o": 7,
    "m": 8,
    "s": 9,
}


NORM_PATH_CHARACTERS = {" ", "_", "-", "—", ".", ","}


DownloadTask = namedtuple("DownloadTask",
                          ["parent_type", "parent_id", "obj_type", "obj_id", "url", "dump_fname"])
AttachDescription = namedtuple("AttachDescription", ["parent_type", "parent_id", "obj_type", "text"])


@dataclass
class Downloader:
    dump_dir_name: str
    tasks: list[DownloadTask] = field(default_factory=list)

    def add_task(self, parent_type: str, parent_id: int | None, obj_type: str,
                 obj_id: int, url: str, dump_fname: str):
        self.tasks.append(DownloadTask(parent_type, parent_id, obj_type, obj_id, url, dump_fname))

    async def _download_file(self, task: DownloadTask, session: aiohttp.ClientSession):

        async with session.get(task.url) as resp:
            if resp.status != 200:
                logger.error("Failed to download file from %s%s with obj id %s, url %s",
                             task.parent_type, task.parent_id, task.obj_id, task.url)
            data = await resp.read()

        out_fname = path.join(self.dump_dir_name, task.dump_fname)
        async with aiofiles.open(out_fname, "wb") as outfile:
            await outfile.write(data)

    async def _download_files_async(self):
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[self._download_file(task, session) for task in self.tasks])

    def download_files(self):
        makedirs(self.dump_dir_name, exist_ok=True)
        asyncio.run(self._download_files_async())
        logger.info("Downloading to the `%s` folder is complete", self.dump_dir_name)


@dataclass
class NofileAttachmentsReporter:
    dump_dir_name: str
    attachments: list[AttachDescription] = field(default_factory=list)

    def add_attachment(self, parent_type: str, parent_id: int | None, obj_type: str, text: str):
        self.attachments.append(AttachDescription(parent_type, parent_id, obj_type, text))

    def dump_attachments(self):
        out_fname = path.join(self.dump_dir_name, "attachments.txt")
        with open(out_fname, "w") as fd:
            for att in self.attachments:
                line = "{}\t{}\t{}\t{}\n".format(att.parent_type, att.parent_id, att.obj_type, att.text)
                fd.write(line)
        logger.info("Description of no-file attachments is saved in %s", out_fname)


@dataclass
class FilesCollector:
    owner_id: int

    def download_banner(self):
        dir_name = get_dname_for_data(self.owner_id)
        group_info = path.join(dir_name, get_fname_for_method_result(self.owner_id, "groups.getById"))
        if path.exists(group_info):
            with open(group_info, "r") as fd:
                group = json.load(fd)
            covers = group[0].get("cover", {}).get("images")
            if covers:
                sorted_covers = sorted(covers, key=lambda x: -x["width"])
                url = sorted_covers[0]["url"]
                response = requests.get(url)
                ext = path.splitext(url)[1].split("?")[0]
                out_fname = path.join(dir_name, f"banner{ext}")
                with open(out_fname, "wb") as fd:
                    fd.write(response.content)
                logger.info("Banner is downloaded to `%s`", out_fname)

    @staticmethod
    def _get_url_and_extenion_by_photo(photo: dict) -> tuple[str | None, str | None]:
        sizes = photo.get("sizes")
        if sizes:
            sorted_sizes = sorted(sizes, key=lambda x: IMAGE_TYPES.get(x.get("type")))
            url = sorted_sizes[0].get("url")
            ext = path.splitext(url)[1].split("?")[0]
            return url, ext
        else:
            return None, None

    @staticmethod
    def _norm_path(filename: str) -> str:
        return "".join(c for c in filename if c.isalnum() or c in NORM_PATH_CHARACTERS).rstrip()

    def _collect_attach_tasks(self, parent_obj: dict, parent_type: str, downloader: Downloader,
                              nofile_attach_reporter: NofileAttachmentsReporter, skipped_types: set):

        if "attachments" not in parent_obj:
            return

        for att in parent_obj["attachments"]:
            obj_type = att["type"]
            if obj_type in ("video", "audio", "link"):
                # No-file attachment
                if obj_type == "video":
                    text = att["video"]["title"]
                elif obj_type == "audio":
                    text = "{} — {}".format(att["audio"]["artist"], att["audio"]["title"])
                elif obj_type == "link":
                    text = "{} [{}]".format(att["link"]["title"], att["link"]["url"])
                else:
                    raise UnreachableCode
                nofile_attach_reporter.add_attachment(parent_type, parent_obj["id"], obj_type, text)
            elif obj_type in ("photo", "doc"):
                # File attachment
                obj_id = att[obj_type]["id"]
                if obj_type == "photo":
                    url, ext = self._get_url_and_extenion_by_photo(att["photo"])
                    if not url:
                        logger.warning("Empty photo object (%s id %s, id %s)",
                                       parent_type, parent_obj["id"], obj_id)
                        continue
                    dump_fname = "{}{}_{}{}{}".format(parent_type, parent_obj["id"], obj_type, obj_id, ext)
                elif obj_type == "doc":
                    url = att["doc"]["url"]
                    title = self._norm_path(att["doc"]["title"])
                    ext = ".{}".format(att["doc"]["ext"])
                    if title[-len(ext):] != ext:
                        title += ext
                    dump_fname = "{}{}_{}{}_{}".format(parent_type, parent_obj["id"], obj_type, obj_id, title)
                else:
                    raise UnreachableCode
                downloader.add_task(parent_type, parent_obj["id"], obj_type, obj_id, url, dump_fname)
            else:
                if obj_type not in skipped_types:
                    logger.warning("Skip not supported type of extension `%s` (%s %s)",
                                   obj_type, parent_type, parent_obj["id"])
                    skipped_types.add(obj_type)

    def download_attachments(self):
        """
        Read saved dumps of wall, board, photo albums and fetch information
        about attachments. Next, download attachment files whose download is supported
        by the VK API, save the information about other attachments to text file.
        """

        dump_dir_name = path.join(get_dname_for_data(self.owner_id), "attachments")
        downloader = Downloader(dump_dir_name)
        nofile_attach_reporter = NofileAttachmentsReporter(get_dname_for_data(self.owner_id))
        skipped_types: set[str] = set()
        dir_name = get_dname_for_data(self.owner_id)

        # Wall posts & comments attachments
        wall_fname = path.join(dir_name, get_fname_for_method_result(self.owner_id, "wall.get"))
        if path.exists(wall_fname):
            with open(wall_fname, "r") as fd:
                wall = json.load(fd)
            for post in wall["items"]:
                self._collect_attach_tasks(post, "post", downloader, nofile_attach_reporter, skipped_types)
                if "comments_list" in post:
                    for comm in post["comments_list"]["items"]:
                        self._collect_attach_tasks(comm, "comment", downloader, nofile_attach_reporter,
                                                   skipped_types)

        # Board comments attachments
        board_fname = path.join(dir_name, get_fname_for_method_result(self.owner_id, "board.getTopics"))
        if path.exists(board_fname):
            with open(board_fname, "r") as fd:
                boards = json.load(fd)
            for topic in boards["items"]:
                if "topics_info" in topic:
                    for comm in topic["topics_info"]["items"]:
                        self._collect_attach_tasks(comm, "brd_com", downloader, nofile_attach_reporter,
                                                   skipped_types)

        # Download attachments
        nofile_attach_reporter.dump_attachments()
        downloader.download_files()

    def download_photos(self):
        """
        Read saved result of `photos.getAlbums` method and download all photos.
        """
        albums_fname = path.join(get_dname_for_data(self.owner_id),
                                 get_fname_for_method_result(self.owner_id, "photos.getAlbums"))
        if not path.exists(albums_fname):
            return
        with open(albums_fname, "r") as fd:
            albums = json.load(fd)

        for album in albums["items"]:
            dump_dir_name = path.join(get_dname_for_data(self.owner_id), "photos",
                                      self._norm_path(album["title"]))
            downloader = Downloader(dump_dir_name)
            for photo in album["photos_list"]["items"]:
                url, ext = self._get_url_and_extenion_by_photo(photo)
                if not url:
                    logger.warning("Empty photo object (album id %s, photo id %s)", album["id"], photo["id"])
                    continue
                dump_fname = "a{}_p{}{}".format(album["id"], photo["id"], ext)
                downloader.add_task("album", album["id"], "album_photo", photo["id"], url, dump_fname)
            downloader.download_files()

    def download_docs(self):
        """
        Read saved result of `docs.get` method and download all files.
        """
        docs_fname = path.join(get_dname_for_data(self.owner_id),
                               get_fname_for_method_result(self.owner_id, "docs.get"))
        if not path.exists(docs_fname):
            return
        with open(docs_fname, "r") as fd:
            docs = json.load(fd)

        dump_dir_name = path.join(get_dname_for_data(self.owner_id), "docs")
        downloader = Downloader(dump_dir_name)
        for doc in docs["items"]:
            title = self._norm_path(doc["title"])
            ext = ".{}".format(doc["ext"])
            if title[-len(ext):] != ext:
                title += ext
            dump_fname = "doc{}_{}".format(doc["id"], title)
            downloader.add_task("docs", None, "doc", doc["id"], doc["url"], dump_fname)
        downloader.download_files()
